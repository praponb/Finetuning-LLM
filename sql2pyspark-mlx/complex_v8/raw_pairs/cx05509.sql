WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ABS(premium - 2096.6) AS premium_distance,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m
    FROM policies
    WHERE status = 'ACTIVE'
      AND product = 'LIFE'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER w_start_date_plus_12m_year_sta, 0), 4) AS premium_share_of_start_date_plus_12m_year_status,
        SUM(premium_distance) OVER (PARTITION BY holder_id, product ORDER BY premium_distance ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_distance,
        NTILE(3) OVER (PARTITION BY holder_id ORDER BY start_date, id) AS start_date_tercile
    FROM valid_policys
    WINDOW w_start_date_plus_12m_year_sta AS (PARTITION BY YEAR(start_date_plus_12m), status)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        premium,
        start_date,
        status,
        premium_distance,
        start_date_plus_12m,
        running_premium_distance,
        start_date_tercile,
        ABS(running_premium_distance - 17500.0) AS running_premium_distance_distance,
        ADD_MONTHS(start_date_plus_12m, 1) AS start_date_plus_12m_plus_1m,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM with_windows
),
product_stats AS (
    -- Avg running_premium_distance per product
    SELECT product, AVG(running_premium_distance) AS avg_running_premium_distance
    FROM scored
    GROUP BY product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_running_premium_distance,
        ROUND(s.running_premium_distance - g.avg_running_premium_distance, 2) AS running_premium_distance_minus_avg
    FROM scored s
    JOIN product_stats g ON g.product = s.product
    WHERE s.running_premium_distance > g.avg_running_premium_distance
)
SELECT *
FROM (
    SELECT status, start_date, product, premium_distance
    FROM vs_group
) src
PIVOT (
    MIN(premium_distance) AS min_v, SUM(premium_distance) AS sum_v
    FOR product IN ('AUTO', 'LIFE', 'HOME')
)
