WITH with_dims AS (
    SELECT
        a.*,
        p.country,
        p.city
    FROM products a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
with_last AS (
    SELECT s.*, top1.revenue
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        tags,
        country,
        city,
        LENGTH(city) AS city_length,
        ROUND(cost / revenue, 2) AS cost_per_revenue,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM with_last
    WHERE brand NOT IN ('Umbrella', 'Initech')
),
metrics AS (
    SELECT
        id,
        brand,
        price,
        cost,
        country,
        city,
        city_length,
        city_filled,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY city, country ORDER BY city_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        SUM(cost) OVER (PARTITION BY country, city) AS country_sum_cost
    FROM cleaned
),
remaining_keys AS (
    SELECT brand, id
    FROM metrics
    EXCEPT ALL
    SELECT brand, id
    FROM products
    WHERE price < 370
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.brand = k.brand AND s.id = k.id
)
SELECT *
FROM (
    SELECT
        brand,
        city,
        country,
        price
    FROM reconciled
) src
PIVOT (
    MAX(price) AS total
    FOR country IN ('JP', 'US')
)
