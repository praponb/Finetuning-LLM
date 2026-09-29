WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS rnk
    FROM subscriptions
    WHERE status = 'CANCELLED'
)
SELECT *
FROM numbered
WHERE rnk <= 3;
