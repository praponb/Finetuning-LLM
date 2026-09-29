WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        LOWER(city) AS city_lower,
        CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE country <> 'SG'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        city,
        city_clean,
        CONCAT_WS(',', supplier_name, city) AS supplier_name_city_key,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'NONE') AS resolved_city
    FROM base
    WHERE city IS NOT NULL
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_stock_qty,
        latest.product_cost
    FROM valid s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.city_clean,
        s.supplier_name_city_key,
        s.resolved_city,
        s.product_stock_qty,
        s.product_cost,
        latest.product_cost_2,
        latest.product_price
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost_2, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    supplier_name,
    city,
    city_clean,
    supplier_name_city_key,
    resolved_city,
    product_stock_qty,
    product_cost_2
FROM with_latest
ORDER BY id DESC
LIMIT 20
