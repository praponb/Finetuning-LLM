SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, ROUND(AVG(credit_limit), 2) AS avg_credit_limit FROM customers GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM') ORDER BY signup_month;
