WITH order_stats AS (
    SELECT order_id, COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) bank_transfer_count
    FROM payments
    GROUP BY order_id
)
SELECT o.store_id, os.bank_transfer_count
FROM orders o
LEFT OUTER JOIN order_stats os ON o.id = os.order_id
WHERE os.bank_transfer_count > 5
ORDER BY o.store_id;
