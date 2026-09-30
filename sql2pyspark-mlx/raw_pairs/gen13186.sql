WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_num
        FROM sessions
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    device,
    browser,
    started_at,
    country,
    row_num
FROM deduped;
