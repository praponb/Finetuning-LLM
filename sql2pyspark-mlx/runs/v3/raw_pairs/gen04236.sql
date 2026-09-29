SELECT id, order_date, store_id
FROM orders
WHERE shipping_fee IS NULL
UNION ALL
SELECT id, order_date, store_id
FROM orders
WHERE status != 'RETURNED';
