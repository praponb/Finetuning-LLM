WITH filtered AS (
    SELECT *, GREATEST(cost, stock_qty) greatest_cost_stock_qty
    FROM products
    WHERE launch_date >= DATE '2023-03-19'
)
SELECT s.*
FROM filtered s
WHERE NOT EXISTS (
        SELECT 1
        FROM store_sales c
        WHERE c.product_id = s.id
          AND c.sale_date > '2024-02-01'
    );
