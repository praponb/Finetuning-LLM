WITH base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CAST(stock_qty AS STRING) AS stock_qty_str,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM products
    WHERE launch_date >= DATE '2023-06-16'
      AND supplier_id IS NOT NULL
)
SELECT s.*
FROM base s
WHERE EXISTS (
        SELECT 1
        FROM order_items x
        WHERE x.product_id = s.id
          AND x.order_id IN (25, 26, 35, 36)
    )
