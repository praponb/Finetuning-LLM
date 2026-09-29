SELECT signup_month, segment, total_count
FROM (
    SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, segment, COUNT(*) AS total_count
    FROM customers
    GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM'), segment
) AS s
WHERE total_count < 8
