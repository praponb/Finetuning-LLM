SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS pos
    FROM sessions
) AS t
WHERE pos = 1;
