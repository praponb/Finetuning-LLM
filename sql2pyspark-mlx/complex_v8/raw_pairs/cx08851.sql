WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'LIFE'
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        status,
        premium - LAG(premium) OVER w_status_holder_id_by_end_date AS premium_change,
        MAX(premium) OVER (PARTITION BY status, holder_id ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2
    FROM with_activity
    WINDOW w_status_holder_id_by_end_date AS (PARTITION BY status, holder_id ORDER BY end_date, id)
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.premium_change,
        s.premium_max_2,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM sequenced s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region = 'Central'
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IN ('delay', 'theft', 'medical')) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count
    FROM joined_holder s
)
-- Final projection
SELECT
    id,
    premium_change,
    premium_max_2,
    claim_claim_type_count,
    LEAST(premium_max_2, premium_change) AS least_premium_max_2_premium_change,
    ABS(premium_change - 2000.0) AS premium_change_distance
FROM with_counts
ORDER BY least_premium_max_2_premium_change DESC NULLS FIRST
