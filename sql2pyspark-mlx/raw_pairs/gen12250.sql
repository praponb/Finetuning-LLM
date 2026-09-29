WITH latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device ORDER BY started_at DESC NULLS FIRST) AS row_num
        FROM sessions
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    customer_id,
    browser,
    row_num
FROM latest
ORDER BY id DESC
