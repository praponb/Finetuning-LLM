WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE (product <> 'HOME' OR status = 'LAPSED')
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        product,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(premium) AS max_premium
    FROM scoped
    GROUP BY holder_id, DATE_TRUNC('MONTH', start_date), product
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, MIN(premium) AS min_policy_premium
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.min_policy_premium
    FROM summary s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
policy_stats_2 AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(end_date) AS first_policy_end_date,
        ROUND(AVG(premium), 2) AS avg_policy_premium
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_end_date,
        p.avg_policy_premium
    FROM with_counts s
    LEFT JOIN policy_stats_2 p ON p.holder_id = s.holder_id
)
SELECT *
FROM (
    SELECT start_date_month, distinct_ids, product, min_policy_premium
    FROM with_stats
) src
PIVOT (
    COUNT(min_policy_premium)
    FOR product IN ('HOME', 'AUTO', 'HEALTH', 'LIFE')
)
ORDER BY start_date_month, distinct_ids
