SELECT channel, status, LENGTH(status) AS status_length
FROM orders
WHERE YEAR(order_date) >= 2025 AND amount < 410
ORDER BY channel;
