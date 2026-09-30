SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) rn
    FROM sessions
) t
WHERE rn <= 3
