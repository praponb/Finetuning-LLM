SELECT * FROM (SELECT channel, status, discount FROM orders) PIVOT (COUNT(discount) FOR status IN ('CANCELLED', 'PENDING', 'SHIPPED', 'COMPLETED')) ORDER BY channel
