SELECT * FROM (SELECT channel, status, amount FROM orders WHERE customer_id = 30) PIVOT (AVG(amount) FOR status IN ('SHIPPED', 'CANCELLED', 'PENDING', 'RETURNED')) ORDER BY channel
