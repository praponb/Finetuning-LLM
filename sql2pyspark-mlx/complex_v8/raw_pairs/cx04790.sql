WITH unioned AS (
    -- Stack the branches
    SELECT
        'tariff' AS source,
        id,
        rate,
        tariff
    FROM tariff_windows
    UNION ALL
    SELECT
        'tariff_2' AS source,
        id,
        rate,
        tariff
    FROM tariff_windows
    WHERE TO_DATE(valid_to) < DATE '2025-03-09'
      AND HOUR(valid_from) BETWEEN 6 AND 9
)
SELECT source, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY source
ORDER BY source
