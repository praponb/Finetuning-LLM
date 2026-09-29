SELECT status, channel
FROM orders
WHERE discount IS NOT NULL
ORDER BY channel DESC
