WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        CASE WHEN status IN ('REJECTED', 'PAID') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM claims
    WHERE claim_type = 'delay'
      AND adjuster_id IS NOT NULL
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status_group,
        s.approved_amount_filled,
        s.days_since_claim_date,
        p.start_date AS policy_start_date,
        p.product AS policy_product,
        po.risk_score AS holder_risk_score,
        po.region AS holder_region
    FROM recent s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_product, holder_region ORDER BY days_since_claim_date, id), 3) AS days_since_claim_date_cume_dist,
        ROUND(AVG(days_since_claim_date) OVER w_policy_product_adjuster_id, 2) AS policy_product_adjuster_id_avg_days_since_claim_,
        LAG(claimed_amount) OVER (PARTITION BY policy_product, holder_region ORDER BY days_since_claim_date, id) AS prev_claimed_amount
    FROM joined_policy
    WINDOW w_policy_product_adjuster_id AS (PARTITION BY policy_product, adjuster_id)
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status_group,
        s.approved_amount_filled,
        s.policy_start_date,
        s.policy_product,
        s.holder_risk_score,
        s.policy_product_adjuster_id_avg_days_since_claim_,
        s.prev_claimed_amount,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM windowed s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IN ('delay', 'collision', 'fire')) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_reported_at,
        (SELECT COUNT(CASE WHEN c.claim_type = 'fire' THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count_2
    FROM with_details_adjuster s
),
unique_rows AS (
    -- Keep the latest row per policy_product, adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status_group,
        approved_amount_filled,
        policy_start_date,
        policy_product,
        holder_risk_score,
        policy_product_adjuster_id_avg_days_since_claim_,
        prev_claimed_amount,
        adjuster_level,
        adjuster_team,
        adjuster_name,
        claim_claim_type_count,
        first_claim_reported_at,
        claim_claim_type_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_product, adjuster_team ORDER BY policy_start_date DESC, id DESC) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status_group,
    approved_amount_filled,
    policy_product,
    holder_risk_score,
    policy_product_adjuster_id_avg_days_since_claim_,
    prev_claimed_amount,
    adjuster_level,
    adjuster_team,
    first_claim_reported_at,
    claim_claim_type_count_2
FROM unique_rows
ORDER BY claim_date, first_claim_reported_at NULLS LAST, id
LIMIT 5
