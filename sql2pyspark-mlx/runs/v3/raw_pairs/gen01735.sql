SELECT
    oi.product_id,
    p.product_name,
    p.launch_date,
    s.city,
    oi.unit_price - oi.quantity AS line_total
FROM order_items oi
FULL OUTER JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
