SELECT ROUND(AVG(credit_limit), 2) AS avg_credit_limit, COUNT(*) AS cnt
FROM customers
WHERE country NOT IN ('SG', 'JP', 'UK') AND segment = 'Corporate' AND country NOT IN ('SG', 'TH', 'JP')
ORDER BY cnt;
