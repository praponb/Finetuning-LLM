WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE country = 'US'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND UPPER(ch.product_name) LIKE '%UNN%'
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        city,
        is_singapore,
        city_clean,
        LEAD(is_singapore, 1) OVER (ORDER BY is_singapore, id) AS next_is_singapore,
        AVG(is_singapore) OVER (PARTITION BY city ORDER BY is_singapore, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS is_singapore_avg_5,
        LAST_VALUE(is_singapore) IGNORE NULLS OVER (ORDER BY is_singapore, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_singapore
    FROM eligible
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Globex', 'Acme')
        )
)
SELECT
    id,
    supplier_name,
    is_singapore,
    city_clean,
    is_singapore_avg_5
FROM kept
ORDER BY id DESC
