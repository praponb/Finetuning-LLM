SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) rnk
    FROM subscriptions
) t
WHERE rnk = 1;
