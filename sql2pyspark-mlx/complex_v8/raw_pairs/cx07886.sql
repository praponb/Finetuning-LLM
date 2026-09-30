WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM policies
    WHERE premium < 3719.7
      AND product IN ('HEALTH', 'HOME', 'LIFE')
),
with_activity AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.joined_on < DATE '2025-02-22'
              AND p.region NOT IN ('Central', 'South')
        )
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date_month,
        p.risk_score AS holder_risk_score
    FROM with_activity s
    JOIN policyholders p ON p.id = s.holder_id
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.holder_risk_score,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score_2,
        p.region AS holder_region
    FROM combined_details_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_start_date,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_2
    FROM enriched s
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.holder_segment,
        s.holder_risk_score_2,
        s.holder_region,
        s.last_policy_start_date,
        s.policy_count,
        s.policy_count_2,
        p.segment AS holder_segment_2,
        p.holder_name
    FROM with_stats s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
tagged AS (
    -- Derive helper columns
    SELECT *, ROUND(holder_risk_score_2 * 1.5, 2) AS holder_risk_score_2_adj
    FROM with_details_holder
    WHERE policy_count_2 <= 6
)
SELECT id, field, amount
FROM (
    SELECT id, policy_count, policy_count_2
    FROM tagged
) src
UNPIVOT (
    amount FOR field IN (policy_count AS policy_count_v, policy_count_2 AS policy_count_2_v)
) unp
ORDER BY id, field
