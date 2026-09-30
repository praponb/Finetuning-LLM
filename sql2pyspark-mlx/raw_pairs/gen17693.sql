WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN country IN ('SG', 'TH') THEN 'Core' ELSE 'Other' END AS country_group,
        CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore
    FROM suppliers
    WHERE city = 'New York'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, city
    FROM valid
    EXCEPT
    SELECT country, city
    FROM suppliers
    WHERE supplier_name LIKE '%Imports%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.country = k.country AND s.city = k.city
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.stock_qty < 220
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date < '2023-08-15'
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        is_singapore,
        STDDEV_SAMP(rating) OVER (PARTITION BY country, city) AS country_stddev_samp_rating,
        RANK() OVER w_hist AS is_singapore_rank,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY is_singapore DESC NULLS LAST) AS is_singapore_dense_rank,
        RANK() OVER w_ordered AS rating_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
    FROM kept
    WINDOW w_hist AS (PARTITION BY city, country ORDER BY is_singapore NULLS LAST), w_ordered AS (ORDER BY rating DESC NULLS LAST)
),
kept_suppliers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty IS NOT NULL
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(city) AS city_length,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM kept_suppliers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'London'
),
daily AS (
    -- Grouped metrics
    SELECT
        city,
        COUNT_IF(city = 'New York') AS new_york_events,
        SUM(CASE WHEN city = 'Singapore' THEN city_length ELSE 0 END) AS singapore_city_length,
        MAX(is_singapore_dense_rank) AS max_is_singapore_dense_rank,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM base
    GROUP BY city
)
SELECT city, singapore_city_length
FROM daily
ORDER BY singapore_city_length, city DESC
