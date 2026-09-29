SELECT id, status, end_date
FROM (
    SELECT
        id,
        status,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS rnk
    FROM subscriptions
) AS ranked
WHERE rnk <= 1
