SELECT
    id,
    country,
    started_at,
    browser,
    device,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_num,
    FIRST_VALUE(device) OVER (PARTITION BY browser ORDER BY started_at, id) AS first_device
FROM sessions
WHERE browser <> 'Chrome';
