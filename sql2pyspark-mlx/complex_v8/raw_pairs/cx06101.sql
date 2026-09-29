WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        COALESCE(city, 'UNKNOWN') AS city_or_unknown,
        DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on,
        DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on_2
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.moved_at < TIMESTAMP '2025-03-02 10:00:00'
        )
),
city_or_unknown_stats AS (
    -- Median days_since_opened_on_2 per city_or_unknown
    SELECT city_or_unknown, PERCENTILE_APPROX(days_since_opened_on_2, 0.5) AS median_days_since_opened_on_2
    FROM screened
    GROUP BY city_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_opened_on_2,
        ROUND(s.days_since_opened_on_2 - g.median_days_since_opened_on_2, 2) AS days_since_opened_on_2_minus_median
    FROM screened s
    JOIN city_or_unknown_stats g ON g.city_or_unknown = s.city_or_unknown
    WHERE s.days_since_opened_on_2 >= g.median_days_since_opened_on_2
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        city_or_unknown,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        MIN(days_since_opened_on_2_minus_median) AS min_days_since_opened_on_2_minus_median
    FROM outliers
    GROUP BY city_or_unknown, DATE_TRUNC('MONTH', opened_on)
)
-- Final projection
SELECT
    city_or_unknown,
    opened_on_month,
    min_days_since_opened_on_2_minus_median,
    COALESCE(city_or_unknown, 'UNKNOWN') AS city_or_unknown_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(city_or_unknown), ' ', '_')) AS city_or_unknown_slug
FROM stats
ORDER BY min_days_since_opened_on_2_minus_median DESC, opened_on_month DESC NULLS FIRST
