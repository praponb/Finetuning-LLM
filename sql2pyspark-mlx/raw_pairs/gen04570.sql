WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        UPPER(country) AS country_upper
    FROM suppliers
    WHERE rating BETWEEN 2.7 AND 3.4
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        country_upper,
        SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix,
        SUBSTRING(country, 1, 2) AS country_prefix
    FROM cleaned
    WHERE city = 'Berlin'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.product_category,
        x.product_stock_qty
    FROM filtered s
    JOIN LATERAL (
            SELECT c.category AS product_category, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'sale')
        )
)
-- Window metrics over several different partitions
SELECT
    id,
    supplier_name_prefix,
    LEAD(product_stock_qty) OVER w_hist AS next_product_stock_qty,
    ROW_NUMBER() OVER w_hist AS row_seq
FROM kept
WINDOW w_hist AS (PARTITION BY product_category, country ORDER BY product_stock_qty, id);
