WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(kwh, 0.0) AS kwh_filled,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE kwh BETWEEN 35.9 AND 109.0
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(voltage) OVER (ORDER BY read_at, id) AS next_voltage,
        LAG(kwh, 3, 0) OVER (PARTITION BY site_id ORDER BY voltage NULLS LAST, id) AS prev3_kwh,
        NTH_VALUE(read_at, 2) OVER (ORDER BY voltage_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER (PARTITION BY site_id), 0), 4) AS kwh_filled_share_of_site_id,
        ROUND(voltage_filled / NULLIF(SUM(voltage_filled) OVER (PARTITION BY site_id), 0), 4) AS voltage_filled_share_of_site_id,
        NTH_VALUE(voltage_filled, 2) OVER (ORDER BY voltage_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_filled,
        DENSE_RANK() OVER (ORDER BY voltage_filled DESC, id) AS voltage_filled_dense_rank,
        MAX(kwh_filled) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_filled_max_4
    FROM valid_readings
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        kwh_filled,
        voltage_filled,
        next_voltage,
        prev3_kwh,
        second_read_at,
        kwh_filled_share_of_site_id,
        voltage_filled_share_of_site_id,
        second_voltage_filled,
        kwh_filled_max_4
    FROM with_trends
    WHERE voltage_filled_dense_rank <= 3
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    voltage_filled,
    next_voltage,
    prev3_kwh,
    second_read_at,
    kwh_filled_share_of_site_id,
    voltage_filled_share_of_site_id,
    second_voltage_filled,
    kwh_filled_max_4,
    GREATEST(second_voltage_filled, voltage_filled) AS greatest_second_voltage_filled_voltage_filled
FROM top_per_group
ORDER BY greatest_second_voltage_filled_voltage_filled, id
LIMIT 5
