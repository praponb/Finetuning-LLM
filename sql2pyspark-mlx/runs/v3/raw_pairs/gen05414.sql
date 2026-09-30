SELECT
    oi.unit_price,
    oi.product_id,
    o.order_date,
    o.amount,
    p.supplier_id
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE p.supplier_id IS NULL AND p.product_name IS NOT NULL
ORDER BY o.amount ASC
