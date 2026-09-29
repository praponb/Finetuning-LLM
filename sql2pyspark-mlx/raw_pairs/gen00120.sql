WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price <= 830
        )
),
cleaned AS (
    SELECT
        *,
        CASE WHEN country IN ('SG', 'JP') THEN 'Core' ELSE 'Other' END country_group,
        CASE WHEN country IN ('DE', 'TH') THEN 'Priority' ELSE 'Other' END country_group_2
    FROM kept
    WHERE rating <= 2.7
),
valid AS (
    SELECT
        *,
        LENGTH(city) city_length,
        CONCAT_WS('/', supplier_name, country) supplier_name_country_key
    FROM cleaned
    WHERE city = 'New York'
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand = 'Umbrella'
        )
),
eligible_suppliers AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty != 152
        )
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY city_length DESC, id) AS rn
    FROM eligible_suppliers
    QUALIFY rn = 1
),
base AS (
    SELECT *, rn / NULLIF(city_length, 0) rn_per_city_length
    FROM latest
    WHERE city IS NOT NULL
)
SELECT
    city,
    country,
    SIZE(COLLECT_SET(city)) AS num_cities,
    APPROX_PERCENTILE(city_length, 0.95) AS city_length_p95
FROM base
GROUP BY city, country
HAVING APPROX_PERCENTILE(city_length, 0.95) < 1000000
ORDER BY city
