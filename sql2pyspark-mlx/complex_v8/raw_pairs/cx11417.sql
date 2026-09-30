WITH filtered_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE HOUR(valid_from) BETWEEN 10 AND 13
),
combined AS (
    -- Stack the branches
    SELECT id, tariff AS category
    FROM filtered_tariffs
    UNION ALL
    SELECT site_id AS id, quality AS category
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 09:00:00'
      AND voltage BETWEEN 213.2 AND 233.1
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM combined
GROUP BY category
ORDER BY category
