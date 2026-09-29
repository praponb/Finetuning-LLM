WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        opened_date,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY YEAR(opened_date), region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
    FROM stores
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        first_known_sqft,
        first_known_region
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY first_known_sqft NULLS LAST) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY first_known_sqft DESC NULLS FIRST, id) = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.units > 34
        )
),
grouped AS (
    -- Grouped metrics
    SELECT
        city,
        SIZE(COLLECT_SET(city)) AS num_cities,
        APPROX_PERCENTILE(first_known_sqft, ARRAY(0.25, 0.5, 0.75)) AS first_known_sqft_quartiles,
        SIZE(COLLECT_SET(city)) AS num_cities_2,
        SUM(CASE WHEN city = 'Tokyo' THEN first_known_sqft ELSE 0 END) AS tokyo_first_known_sqft
    FROM not_flagged
    GROUP BY 1
    HAVING SIZE(COLLECT_SET(city)) < 1000
)
SELECT
    city,
    num_cities,
    first_known_sqft_quartiles,
    num_cities_2,
    tokyo_first_known_sqft,
    CASE WHEN city RLIKE '^(Chiang Mai|Singapore)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM grouped;
