WITH cleaned_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        ABS(rate - 5.9) AS rate_distance
    FROM tariff_windows
    WHERE rate <= 5.7
),
all_events AS (
    -- Stack the branches
    SELECT
        id AS ref_id,
        valid_from AS event_time,
        tariff,
        'tariff' AS src
    FROM cleaned_tariffs
    WHERE rate_distance BETWEEN 2.0 AND 4.8
      AND tariff = 'TOU'
    UNION ALL
    SELECT
        id,
        valid_from,
        tariff,
        'tariff_2' AS src
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-02-16 11:00:00'
)
SELECT src, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY src
ORDER BY src
