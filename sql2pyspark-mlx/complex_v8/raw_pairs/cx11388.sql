WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM meter_readings
    WHERE site_id >= 2
      AND MONTH(read_at) IN (1, 3)
),
deduped AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        greatest_voltage_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id DESC) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.greatest_voltage_kwh,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM deduped s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.quality NOT IN ('OK', 'BAD')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        voltage,
        quality,
        greatest_voltage_kwh,
        reading_id,
        reading_kwh,
        reading_voltage,
        reading_quality,
        ROW_NUMBER() OVER w_reading_id_by_greatest_volta AS rn,
        NTH_VALUE(voltage, 2) OVER (ORDER BY reading_kwh DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        LAST_VALUE(reading_voltage) IGNORE NULLS OVER (ORDER BY reading_kwh DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reading_voltage,
        COUNT(*) OVER (PARTITION BY YEAR(read_at)) AS rows_in_read_at_year,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS voltage_share_of_read_at_year
    FROM with_children_readings
    WINDOW w_reading_id_by_greatest_volta AS (PARTITION BY reading_id ORDER BY greatest_voltage_kwh ASC, id, reading_id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        voltage,
        quality,
        greatest_voltage_kwh,
        reading_id,
        reading_voltage,
        reading_quality,
        rn,
        second_voltage,
        last_known_reading_voltage,
        rows_in_read_at_year,
        ROUND(voltage * 0.9, 2) AS voltage_adj
    FROM with_windows
)
SELECT id, reading_id, metric, value
FROM (
    SELECT id, reading_id, rows_in_read_at_year, rn
    FROM labeled
) src
UNPIVOT (
    value FOR metric IN (rows_in_read_at_year, rn)
) unp
