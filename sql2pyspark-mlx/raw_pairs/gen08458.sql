WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        MONTH(signup_date) AS signup_date_month,
        COALESCE(city, 'NONE') AS city_clean
    FROM customers
    WHERE signup_date >= DATE '2023-06-17'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(city) AS clean_city
    FROM not_flagged
    WHERE city IS NOT NULL
      AND signup_date > DATE '2023-03-15'
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM base
    EXCEPT DISTINCT
    SELECT city, id
    FROM customers
    WHERE is_active
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.city = k.city AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country ORDER BY signup_date_month DESC NULLS FIRST) AS signup_date_month_cume_dist,
        RANK() OVER (PARTITION BY country, city ORDER BY signup_date_month DESC NULLS LAST) AS signup_date_month_rank
    FROM kept_rows
),
base_customers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(segment, 'N/A') AS segment_filled,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM with_windows
    WHERE signup_date >= '2024-12-01'
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        city,
        COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
        SIZE(COLLECT_SET(city)) AS num_cities,
        COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM base_customers
    GROUP BY city
)
SELECT
    city,
    consumer_rows,
    num_cities,
    corporate_count,
    CASE WHEN city RLIKE '^(Tokyo|New York)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS city_zone
FROM grouped
ORDER BY city_zone DESC
