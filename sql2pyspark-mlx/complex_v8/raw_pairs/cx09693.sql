WITH stacked AS (
    -- Stack the branches
    SELECT tariff AS category, valid_from
    FROM tariff_windows
    UNION ALL
    SELECT tariff AS category, valid_to AS valid_from
    FROM tariff_windows
    UNION ALL
    SELECT tariff AS category, valid_from
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 10 AND 14
      AND valid_from >= TIMESTAMP '2025-01-26 03:00:00'
)
SELECT category, COUNT(*) AS n_rows, MAX(valid_from) AS latest_valid_from
FROM stacked
GROUP BY category
ORDER BY category
