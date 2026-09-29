WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        LENGTH(product_name) AS product_name_length,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-07-05'
)
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY product_name_length DESC NULLS FIRST) AS rank_in_group
    FROM cleaned
) x
WHERE rank_in_group = 1
