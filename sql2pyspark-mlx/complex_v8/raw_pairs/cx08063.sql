WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        SUBSTRING(TRIM(tariff), 1, 2) AS tariff_prefix,
        DATE_FORMAT(valid_from, 'yyyy-MM-dd') AS valid_from_label
    FROM tariff_windows
    WHERE rate < 5.9
      AND valid_from >= TIMESTAMP '2025-01-23 19:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT *, UPPER(TRIM(tariff_prefix)) AS tariff_prefix_clean
    FROM prepared
)
SELECT
    id,
    tariff,
    valid_from,
    valid_from_label,
    tariff_prefix_clean
FROM tagged
WHERE valid_from >= TIMESTAMP '2025-01-26 03:00:00'
ORDER BY tariff_prefix_clean, id
LIMIT 5
