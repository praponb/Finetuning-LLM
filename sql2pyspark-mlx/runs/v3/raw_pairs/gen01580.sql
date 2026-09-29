SELECT * FROM (SELECT customer_id, status, balance FROM accounts) PIVOT (SUM(balance) FOR status IN ('OPEN', 'CLOSED')) ORDER BY customer_id;
