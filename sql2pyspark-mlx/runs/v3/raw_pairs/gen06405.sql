SELECT
    id,
    customer_id,
    started_at,
    browser
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) rn
    FROM sessions
) ranked
WHERE rn <= 1;
