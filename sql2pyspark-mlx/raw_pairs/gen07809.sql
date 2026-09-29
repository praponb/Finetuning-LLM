SELECT *
FROM (
    SELECT YEAR(end_date) AS end_year, status, COUNT(monthly_fee) AS monthly_fee_count
    FROM subscriptions
    GROUP BY YEAR(end_date), status
) AS t
WHERE monthly_fee_count >= 7;
