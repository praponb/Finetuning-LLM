SELECT
    id,
    country,
    started_at,
    browser
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_num
    FROM sessions
) AS t
WHERE row_num <= 1
ORDER BY country DESC, started_at ASC
