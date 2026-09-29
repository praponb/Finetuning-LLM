SELECT id, country, started_at
FROM (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS rnk
    FROM sessions
) AS t
WHERE rnk <= 3;
