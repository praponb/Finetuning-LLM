SELECT
    o.shipping_fee,
    o.store_id,
    o.amount,
    p.status
FROM orders o
LEFT OUTER JOIN payments p ON o.id = p.order_id
ORDER BY shipping_fee ASC
