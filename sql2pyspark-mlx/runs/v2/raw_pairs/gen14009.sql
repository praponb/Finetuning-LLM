SELECT SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END) AS th_count, ROUND(AVG(credit_limit), 2) AS avg_credit_limit FROM customers WHERE age = 46 OR age BETWEEN 40 AND 63 ORDER BY th_count DESC
