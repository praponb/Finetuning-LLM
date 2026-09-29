WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        YEAR(reported_at) AS reported_at_year
    FROM claims
    WHERE policy_id >= 15
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount_plus_claimed_amount,
        claim_type_len,
        reported_at_year,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date)), 2) AS policy_id_claim_date_month_avg_approved_amount,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY reported_at_year ORDER BY approved_amount_plus_claimed_amount DESC NULLS FIRST, id)) AS days_since_prev_claim_date,
        RANK() OVER (PARTITION BY reported_at_year ORDER BY approved_amount_plus_claimed_amount DESC NULLS FIRST, id) AS approved_amount_plus_claimed_amount_rank,
        DENSE_RANK() OVER (PARTITION BY reported_at_year ORDER BY approved_amount_plus_claimed_amount DESC NULLS FIRST, id) AS approved_amount_plus_claimed_amount_dense_rank
    FROM prepared
),
with_parent_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_type,
        s.approved_amount_plus_claimed_amount,
        s.claim_type_len,
        s.reported_at_year,
        s.policy_id_claim_date_month_avg_approved_amount,
        s.approved_amount_plus_claimed_amount_rank,
        s.approved_amount_plus_claimed_amount_dense_rank,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM metrics s
    LEFT JOIN policies p ON p.id = s.policy_id
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent_policy s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    policy_holder_id,
    claim_type_len,
    policy_id,
    SUM(claim_type_len) FILTER (WHERE claim_type = 'delay') AS claim_type_len_claim_type_delay,
    ROUND(100 * COUNT(CASE WHEN claim_type <> 'fire' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_fire
FROM qualified
GROUP BY GROUPING SETS ((policy_holder_id, claim_type_len, policy_id), (policy_holder_id), (claim_type_len), ())
ORDER BY policy_holder_id NULLS LAST, claim_type_len NULLS LAST, policy_id NULLS LAST
