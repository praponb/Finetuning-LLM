WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality <> 'BAD'
      AND site_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Join sites
    SELECT s.*, si.tariff AS site_tariff
    FROM unique_rows s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw <= 386.6
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_site_tariff_by_voltage)) / 60, 1) AS minutes_since_prev_read_at,
        NTILE(5) OVER (PARTITION BY site_tariff ORDER BY voltage DESC NULLS FIRST, id) AS voltage_quintile
    FROM joined
    WINDOW w_site_tariff_by_voltage AS (PARTITION BY site_tariff ORDER BY voltage DESC NULLS FIRST, id)
),
combined_details AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw <= 446.9
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, quality, kwh
    FROM combined_details
) src
PIVOT (
    AVG(kwh)
    FOR quality IN ('EST', 'BAD', 'OK')
)
ORDER BY site_id
