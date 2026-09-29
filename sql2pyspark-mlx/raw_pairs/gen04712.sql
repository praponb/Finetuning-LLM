SELECT COUNT(amount) AS amount_count, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped FROM orders WHERE store_id IS NULL AND channel = 'web';
