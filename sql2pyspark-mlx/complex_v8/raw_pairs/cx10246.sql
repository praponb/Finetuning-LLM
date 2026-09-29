WITH stacked AS (
    -- Stack the branches
    SELECT id, commissioned_on AS day, tariff
    FROM sites
    UNION ALL
    SELECT id, commissioned_on, tariff
    FROM sites
)
SELECT tariff, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(day) AS latest_day
FROM stacked
GROUP BY tariff
ORDER BY tariff
