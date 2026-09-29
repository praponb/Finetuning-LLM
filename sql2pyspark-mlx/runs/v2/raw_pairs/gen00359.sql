SELECT
    id,
    customer_id,
    started_at,
    country
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rnk
    FROM sessions
) AS ranked
WHERE rnk <= 5;
