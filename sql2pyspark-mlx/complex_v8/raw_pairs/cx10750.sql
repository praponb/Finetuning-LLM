WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ADD_MONTHS(end_date, 12) AS end_date_plus_12m,
        CASE WHEN premium >= 3521.8 THEN 'large' WHEN premium >= 2096.6 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE start_date BETWEEN DATE '2024-07-19' AND DATE '2025-05-28'
      AND status IN ('CANCELLED', 'LAPSED')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claim_date) AS last_claim_date,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE claim_type IN ('theft', 'delay', 'medical')) AS claim_claim_type_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM base_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    premium_tier,
    claim_count,
    claim_claim_type_count,
    GROUPING_ID() AS gid,
    MIN(premium) AS min_premium,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM profiled
GROUP BY ROLLUP(premium_tier, claim_count, claim_claim_type_count)
