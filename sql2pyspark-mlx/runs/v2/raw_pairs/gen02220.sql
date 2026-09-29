SELECT * FROM (SELECT customer_id, status, balance FROM accounts WHERE opened_date >= '2024-11-07') PIVOT (AVG(balance) FOR status IN ('CLOSED', 'FROZEN')) ORDER BY customer_id;
