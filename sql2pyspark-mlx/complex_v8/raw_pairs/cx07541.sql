WITH combined AS (
    -- Stack the branches
    SELECT id, rate AS value
    FROM tariff_windows
    WHERE MONTH(valid_to) IN (2, 3)
    UNION ALL
    SELECT id, installed_kw AS value
    FROM sites
)
SELECT id, COUNT(*) AS n_rows, SUM(value) AS total_value
FROM combined
GROUP BY id
ORDER BY id
