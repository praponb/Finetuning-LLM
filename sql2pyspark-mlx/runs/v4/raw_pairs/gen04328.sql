SELECT
    channel,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows,
    ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS discount_per_returned
FROM orders
GROUP BY channel
ORDER BY channel DESC;
