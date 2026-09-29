SELECT
    id,
    customer_id,
    started_at,
    browser
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS row_num
    FROM sessions
) AS t
WHERE row_num <= 5
ORDER BY started_at, id;
