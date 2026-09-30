WITH valid_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-02-16 11:00:00'
      AND tariff = 'EV'
),
derived AS (
    -- Add calculated columns
    SELECT *, ABS(rate - 5.9) AS rate_distance
    FROM valid_tariffs
)
SELECT
    id,
    valid_from,
    valid_to,
    rate_distance,
    DATE_TRUNC('WEEK', valid_from) AS valid_from_week,
    DATE_FORMAT(valid_to, 'yyyy-MM') AS valid_to_label
FROM derived
WHERE TO_DATE(valid_to) < DATE '2025-04-05'
ORDER BY id DESC
