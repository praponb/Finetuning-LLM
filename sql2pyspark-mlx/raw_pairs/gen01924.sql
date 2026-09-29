SELECT segment, SUM(credit_limit) AS total_credit_limit FROM customers GROUP BY segment ORDER BY total_credit_limit ASC
