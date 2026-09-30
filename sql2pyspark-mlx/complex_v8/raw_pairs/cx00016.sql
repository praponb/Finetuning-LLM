WITH scoped_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        COALESCE(rate, 0.0) AS rate_filled
    FROM tariff_windows
    WHERE rate <= 6.1
      AND valid_from < TIMESTAMP '2025-03-09 04:00:00'
),
all_events AS (
    -- Stack the branches
    SELECT id, rate, tariff
    FROM tariff_windows
    WHERE tariff = 'FLAT'
      AND MONTH(valid_from) IN (1, 3)
    UNION
    SELECT id, rate, tariff
    FROM tariff_windows
)
SELECT tariff, COUNT(*) AS n_rows, SUM(rate) AS total_rate
FROM all_events
GROUP BY tariff
ORDER BY tariff
