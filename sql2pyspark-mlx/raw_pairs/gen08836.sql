WITH base AS (
    SELECT
        *,
        TRIM(product_name) AS clean_product_name,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE brand IS DISTINCT FROM 'Umbrella'
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    tags,
    supplier_id,
    brand_clean,
    CUME_DIST() OVER (PARTITION BY category ORDER BY price DESC NULLS FIRST) AS price_cume_dist,
    PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY brand, category ORDER BY stock_qty, id) AS seq_no
FROM base
