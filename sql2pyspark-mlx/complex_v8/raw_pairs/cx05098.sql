WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE quality IN ('OK', 'BAD')
),
unioned AS (
    -- Stack the branches
    SELECT quality AS label, site_id
    FROM cleaned_readings
    WHERE kwh BETWEEN 9.5 AND 87.1
      AND site_id >= 7
    UNION ALL
    SELECT tariff AS label, id AS site_id
    FROM tariff_windows
    WHERE rate <= 6.1
      AND tariff IS NOT NULL
)
SELECT label, COUNT(*) AS n_rows, SUM(site_id) AS total_site_id
FROM unioned
GROUP BY label
ORDER BY label
