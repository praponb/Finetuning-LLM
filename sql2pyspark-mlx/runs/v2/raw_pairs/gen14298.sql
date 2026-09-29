SELECT
    channel,
    store_id,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) web_count,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) returned_count
FROM orders
WHERE status IN ('PENDING', 'COMPLETED') AND store_id = 29
GROUP BY channel, store_id
ORDER BY web_count
