WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE (kwh >= 51.0 OR site_id >= 6)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY voltage) AS voltage_rank,
        ROW_NUMBER() OVER (ORDER BY voltage, id) AS voltage_seq,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY MONTH(read_at), site_id)) AS distinct_quality_in_read_at_month_site_id,
        LAST_VALUE(read_at) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        NTILE(10) OVER w_all_by_voltage AS voltage_decile,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY MONTH(read_at), site_id), 0), 4) AS voltage_share_of_read_at_month_site_id
    FROM base_readings
    WINDOW w_all_by_voltage AS (ORDER BY voltage, id)
),
enriched AS (
    -- Attach site attributes
    SELECT s.*, si.tariff AS site_tariff
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND MONTH(m.read_at) IN (2, 3)
              AND m.quality IN ('OK', 'EST')
        )
      AND s.quality = 'BAD'
),
scored AS (
    -- Derive helper columns
    SELECT *, kwh_per_voltage - voltage AS kwh_per_voltage_minus_voltage
    FROM filtered
),
latest AS (
    -- Keep the latest row per site_tariff
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_ref,
        kwh_per_voltage,
        voltage_rank,
        voltage_seq,
        distinct_quality_in_read_at_month_site_id,
        last_read_at,
        voltage_decile,
        voltage_share_of_read_at_month_site_id,
        site_tariff,
        kwh_per_voltage_minus_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_tariff ORDER BY voltage_seq DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.kwh_per_voltage,
        s.distinct_quality_in_read_at_month_site_id,
        s.voltage_decile,
        s.voltage_share_of_read_at_month_site_id,
        s.kwh_per_voltage_minus_voltage,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM latest s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, field, amount
FROM (
    SELECT id, reading_id, kwh_per_voltage_minus_voltage, voltage_share_of_read_at_month_site_id
    FROM with_children
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (kwh_per_voltage_minus_voltage AS kwh_per_voltage_minus_voltage_v, voltage_share_of_read_at_month_site_id AS voltage_share_of_read_at_month_site_id_v)
) unp
ORDER BY id, reading_id, field
