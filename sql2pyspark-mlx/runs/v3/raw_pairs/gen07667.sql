SELECT status, COUNT(*) AS cnt
FROM orders
WHERE (shipping_fee < 37 AND channel IN ('web', 'mobile')) OR channel NOT IN ('web', 'store', 'partner')
GROUP BY 1
HAVING COUNT(*) > 4
ORDER BY 2 DESC
