SELECT segment, ROUND(AVG(credit_limit), 2) AS avg_credit_limit FROM customers GROUP BY segment HAVING avg_credit_limit >= 70 ORDER BY avg_credit_limit, segment DESC LIMIT 20;
