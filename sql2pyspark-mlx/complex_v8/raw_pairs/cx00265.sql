WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        TRUNC(claim_date, 'MM') AS claim_date_month_start
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-03-02 13:00:00'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount
    FROM recent s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        DATEDIFF(DATE '2025-06-30', claim_date_month_start) AS days_since_claim_date_month_start
    FROM with_stats
    WHERE claim_date >= DATE '2024-06-13'
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.claim_date_month_start,
        s.min_claim_claimed_amount,
        s.approved_amount_adj,
        s.approved_amount_filled,
        s.days_since_claim_date_month_start,
        p.product AS policy_product,
        p.status AS policy_status
    FROM scored s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.status IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per policy_id, status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_date_month_start,
        min_claim_claimed_amount,
        approved_amount_adj,
        approved_amount_filled,
        days_since_claim_date_month_start,
        policy_product,
        policy_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, status ORDER BY claimed_amount DESC, id DESC) AS recency_rank
        FROM combined_details_policy
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    reported_at,
    claim_date_month_start,
    min_claim_claimed_amount,
    approved_amount_adj,
    approved_amount_filled,
    days_since_claim_date_month_start,
    DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at,
    CASE WHEN status IN ('REJECTED', 'APPROVED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
FROM unique_rows
ORDER BY days_since_claim_date_month_start DESC NULLS FIRST, claimed_amount DESC NULLS FIRST, id
LIMIT 10
