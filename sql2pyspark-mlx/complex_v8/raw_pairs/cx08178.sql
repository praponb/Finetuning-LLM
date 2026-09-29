WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage - kwh AS voltage_minus_kwh,
        ABS(voltage - 231.7) AS voltage_distance
    FROM meter_readings
    WHERE quality NOT IN ('BAD', 'EST')
      AND read_at < TIMESTAMP '2025-02-18 07:00:00'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY YEAR(read_at) ORDER BY voltage_minus_kwh ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        RANK() OVER (PARTITION BY YEAR(read_at) ORDER BY voltage_minus_kwh ASC NULLS LAST, id) AS voltage_minus_kwh_rank,
        ROW_NUMBER() OVER w_read_at_year_by_voltage_minu_2 AS seq_no
    FROM filtered_readings
    WINDOW w_read_at_year_by_voltage_minu_2 AS (PARTITION BY YEAR(read_at) ORDER BY voltage_minus_kwh ASC NULLS LAST, id)
),
joined_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff, si.site_name
    FROM with_windows s
    JOIN sites si ON si.id = s.site_id AND si.zone = 'Z4'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN site_tariff IN ('TOU', 'EV') THEN 'group_a' WHEN site_tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS site_tariff_group,
        ROUND(last_known_voltage * 1.1, 2) AS last_known_voltage_adj,
        ROUND(kwh / NULLIF(voltage_minus_kwh_rank, 0), 3) AS kwh_per_voltage_minus_kwh_rank
    FROM joined_site
    WHERE kwh <= 79.4
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, site_tariff_group, seq_no
    FROM labeled
) src
PIVOT (
    MAX(seq_no)
    FOR site_tariff_group IN ('group_b', 'group_a')
)
ORDER BY read_at
