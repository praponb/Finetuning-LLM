SELECT s.*
FROM products s
WHERE NOT EXISTS (
        SELECT 1
        FROM store_sales c
        WHERE c.product_id = s.id
          AND c.sale_date <= s.launch_date
    )
