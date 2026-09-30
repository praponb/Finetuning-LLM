WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE premium < 3465.4
      AND status = 'ACTIVE'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MAX(start_date) AS last_policy_start_date,
        COUNT(CASE WHEN product = 'AUTO' THEN 1 END) AS policy_product_count,
        MAX(premium) AS max_policy_premium
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.last_policy_start_date,
        COALESCE(p.policy_product_count, 0) AS policy_product_count,
        p.max_policy_premium
    FROM scoped s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
holder_id_stats AS (
    -- Max max_policy_premium per holder_id
    SELECT holder_id, MAX(max_policy_premium) AS max_max_policy_premium
    FROM with_stats
    GROUP BY holder_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_max_policy_premium,
        ROUND(s.max_policy_premium - g.max_max_policy_premium, 2) AS max_policy_premium_minus_max
    FROM with_stats s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.max_policy_premium = g.max_max_policy_premium
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region = 'South'
        )
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY policy_product_count, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        ROUND(CUME_DIST() OVER w_policy_product_count_status_2, 3) AS start_date_cume_dist,
        ROUND(AVG(max_max_policy_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product), 2) AS start_date_month_product_avg_max_max_policy_prem,
        RANK() OVER w_policy_product_count_status_2 AS start_date_rank
    FROM filtered
    WINDOW w_policy_product_count_status_2 AS (PARTITION BY policy_product_count, status ORDER BY start_date)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    last_policy_start_date,
    policy_product_count,
    max_policy_premium,
    max_max_policy_premium,
    max_policy_premium_minus_max,
    first_known_end_date,
    start_date_month_product_avg_max_max_policy_prem,
    start_date_rank
FROM metrics
ORDER BY max_policy_premium DESC, status, id
LIMIT 20
