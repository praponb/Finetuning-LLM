SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    o.discount,
    o.store_id,
    p.price,
    p.cost,
    CEIL(oi.quantity) AS quantity_whole
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN products p ON oi.product_id = p.id
WHERE o.channel NOT IN ('store', 'partner')
ORDER BY quantity_whole ASC, id ASC
LIMIT 5
