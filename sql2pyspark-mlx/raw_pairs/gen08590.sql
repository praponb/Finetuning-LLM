WITH valid AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CONCAT_WS('-', brand, product_name) brand_product_name_key,
        FLOOR(stock_qty) stock_qty_whole,
        COALESCE(brand, 'UNKNOWN') resolved_brand
    FROM products
    WHERE launch_date >= TO_DATE('2023-06-08')
      AND LOWER(TRIM(product_name)) = 'wireless mouse'
)
SELECT
    s.*,
    latest.sale_date,
    latest.revenue
FROM valid s
LEFT JOIN LATERAL (
        SELECT c.sale_date, c.revenue
        FROM store_sales c
        WHERE c.product_id = s.id
        ORDER BY c.units DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
