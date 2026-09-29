SELECT
    id,
    browser,
    started_at,
    customer_id
FROM (
    SELECT
        id,
        browser,
        started_at,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS row_num
    FROM sessions
    WHERE browser IN ('Safari', 'Chrome', 'Firefox')
) AS r
WHERE row_num <= 3;
