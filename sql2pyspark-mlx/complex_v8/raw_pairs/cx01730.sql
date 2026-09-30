WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 96.8 THEN 'A' WHEN kwh >= 45.3 THEN 'B' ELSE 'C' END AS kwh_tier,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        ABS(voltage - 225.2) AS voltage_distance
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 09:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM valid
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        *,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h,
        ROUND(kwh * 1.1, 2) AS kwh_adj,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM tagged
),
rollup_base AS (
    -- Group-level totals
    SELECT
        site_id,
        SUM(CASE WHEN quality = 'BAD' THEN 1 ELSE 0 END) AS n_quality_bad,
        COUNT(*) AS cnt,
        SUM(kwh_adj) AS total_kwh_adj,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(*) FILTER (WHERE quality <> 'BAD') AS n_quality_bad_2
    FROM tagged_2
    GROUP BY site_id
)
SELECT site_id, field, reading
FROM (
    SELECT site_id, cnt, n_quality_bad_2
    FROM rollup_base
) src
UNPIVOT (
    reading FOR field IN (cnt AS cnt_v, n_quality_bad_2 AS m_n_quality_bad_2)
) unp
