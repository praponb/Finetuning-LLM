SELECT end_month, row_count
FROM (
    SELECT MONTH(end_date) AS end_month, COUNT(*) AS row_count
    FROM subscriptions
    GROUP BY MONTH(end_date)
) AS agg
WHERE row_count < 7;
