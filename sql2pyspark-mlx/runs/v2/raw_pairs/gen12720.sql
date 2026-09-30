SELECT oi.quantity, p.launch_date, GREATEST(p.price, p.cost) AS greatest_price_cost
FROM order_items oi
JOIN products p ON p.id = oi.product_id
WHERE oi.product_id = 20 AND oi.product_id IS NOT NULL;
