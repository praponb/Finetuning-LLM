WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
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
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj
    FROM claims
    WHERE approved_amount >= 0.0
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM scoped_claims
    WHERE claimed_amount_adj < (SELECT MAX(claimed_amount_adj) FROM scoped_claims)
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM without_matches s
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        claimed_amount_adj,
        claim_count,
        ROUND(approved_amount * 1.07, 2) AS approved_amount_adj,
        FLOOR(claimed_amount_adj / 10000) * 10000 AS claimed_amount_adj_bucket
    FROM with_counts
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status = 'IN_REVIEW'
        )
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(reported_at) AS last_claim_reported_at,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        COUNT(CASE WHEN status IN ('OPEN', 'REJECTED', 'IN_REVIEW') THEN 1 END) AS claim_status_count
    FROM claims
    GROUP BY adjuster_id
),
with_counts_2 AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        c.min_claim_claimed_amount,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM screened s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.adjuster_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LAST_DAY(claim_date) AS claim_date_month_end,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        ROUND((UNIX_TIMESTAMP(last_claim_reported_at) - UNIX_TIMESTAMP(reported_at)) / 60, 1) AS minutes_reported_at_to_last_claim_reported_at
    FROM with_counts_2
    WHERE approved_amount_adj > 18750.0
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    reported_at,
    claimed_amount_adj,
    claim_count,
    approved_amount_adj,
    claim_status_count,
    claim_date_month_end,
    days_since_claim_date,
    minutes_reported_at_to_last_claim_reported_at,
    GREATEST(claim_status_count, days_since_claim_date) AS greatest_claim_status_count_days_since_claim_dat
FROM labeled
ORDER BY adjuster_id DESC, claim_status_count DESC NULLS FIRST, id
LIMIT 20
