SELECT o.status, o.shipping_fee, p.amount
FROM orders o
JOIN payments p ON p.order_id = o.id
WHERE p.order_id IN (5, 14, 30, 38) AND o.store_id IS NULL
ORDER BY amount ASC, status ASC;
