WITH recent_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        ADD_MONTHS(joined_on, 6) AS joined_on_plus_6m,
        DATE_ADD(joined_on, 7) AS joined_on_plus_7d
    FROM policyholders
    WHERE LOWER(holder_name) LIKE '%ak%'
      AND region = 'South'
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS last_policy_end_date,
        (SELECT COUNT(CASE WHEN p.product = 'TRAVEL' THEN 1 END) FROM policies p WHERE p.holder_id = s.id) AS policy_product_count,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.id) AS first_policy_start_date
    FROM recent_holders s
),
policy_stats_2 AS (
    -- Per-holder policies metrics
    SELECT holder_id, MAX(premium) AS max_policy_premium
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.max_policy_premium
    FROM with_counts s
    LEFT JOIN policy_stats_2 p ON p.holder_id = s.id
)
SELECT *
FROM (
    SELECT segment, first_policy_start_date, region, policy_product_count
    FROM profiled
) src
PIVOT (
    MAX(policy_product_count)
    FOR region IN ('Central', 'West', 'North', 'South')
)
ORDER BY segment, first_policy_start_date
