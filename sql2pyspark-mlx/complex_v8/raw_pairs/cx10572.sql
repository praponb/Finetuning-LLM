WITH filtered_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM tariff_windows
    WHERE rate > 2.2
      AND valid_from < TIMESTAMP '2025-03-09 04:00:00'
)
SELECT id AS ref_id, tariff_ref
FROM filtered_tariffs
INTERSECT ALL
SELECT id, tariff
FROM sites
ORDER BY ref_id, tariff_ref
