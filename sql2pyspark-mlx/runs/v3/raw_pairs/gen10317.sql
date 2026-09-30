SELECT
    oi.id,
    oi.unit_price,
    oi.order_id,
    oi.quantity,
    p.product_name,
    p.price,
    GREATEST(p.cost, p.stock_qty) AS greatest_cost_stock_qty
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE p.launch_date < '2023-11-01'
