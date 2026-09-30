SELECT segment, SUM(credit_limit) AS total_credit_limit
FROM customers
WHERE credit_limit BETWEEN 10800 AND 13400 OR country = 'TH'
GROUP BY segment
HAVING total_credit_limit > 54000;
