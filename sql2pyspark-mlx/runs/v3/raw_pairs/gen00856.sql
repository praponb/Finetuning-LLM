SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, COUNT(*) AS cnt
FROM customers
WHERE is_active
GROUP BY signup_month
HAVING COUNT(*) >= 4;
