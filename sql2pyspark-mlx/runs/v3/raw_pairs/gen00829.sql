SELECT DISTINCT channel, status
FROM orders
WHERE status IN ('COMPLETED', 'SHIPPED')
ORDER BY status ASC, channel ASC
LIMIT 20;
