SELECT * FROM (SELECT channel, status, amount FROM orders WHERE customer_id IN (6, 7, 21)) PIVOT (AVG(amount) FOR status IN ('SHIPPED', 'RETURNED', 'CANCELLED', 'COMPLETED')) ORDER BY channel
