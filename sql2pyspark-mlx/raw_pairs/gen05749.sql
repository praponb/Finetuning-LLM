SELECT channel, order_date
FROM orders
WHERE amount < 660
UNION ALL
SELECT channel, order_date
FROM orders
WHERE amount < 610
ORDER BY channel
