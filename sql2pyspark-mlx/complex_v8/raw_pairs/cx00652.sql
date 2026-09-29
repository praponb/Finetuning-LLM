WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        voltage - kwh AS voltage_minus_kwh,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE kwh BETWEEN 10.9 AND 109.0
      AND site_id IS NOT NULL
),
with_activity AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw <= 386.6
              AND si.commissioned_on >= DATE '2024-08-24'
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY voltage_minus_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        RANK() OVER (PARTITION BY quality ORDER BY voltage_minus_kwh, id) AS voltage_minus_kwh_rank,
        kwh - LAG(kwh) OVER (PARTITION BY quality ORDER BY voltage_minus_kwh, id) AS kwh_change
    FROM with_activity
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.quality_or_unknown,
        s.voltage_minus_kwh,
        s.last_known_kwh,
        s.voltage_vs_site_id_avg,
        s.kwh_change,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM with_windows s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        COUNT(DISTINCT quality_or_unknown) AS distinct_quality_or_unknowns,
        COUNT(*) FILTER (WHERE reading_quality IS NOT NULL) AS n_reading_quality_is_not_null,
        MAX(reading_read_at) AS last_reading_read_at,
        ROUND(AVG(voltage_vs_site_id_avg), 2) AS avg_voltage_vs_site_id_avg,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT reading_id) AS distinct_reading_ids
    FROM detail_rows_readings
    GROUP BY quality
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(avg_voltage_vs_site_id_avg) IGNORE NULLS OVER (ORDER BY last_reading_read_at, quality ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_voltage_vs_site_id_avg,
        distinct_reading_ids - LAG(distinct_reading_ids) OVER (PARTITION BY TO_DATE(last_reading_read_at), distinct_quality_or_unknowns ORDER BY distinct_reading_ids DESC, quality) AS distinct_reading_ids_change,
        ROUND(distinct_quality_or_unknowns / NULLIF(SUM(distinct_quality_or_unknowns) OVER w_distinct_quality_or_unknowns, 0), 4) AS distinct_quality_or_unknowns_share_of_distinct_q,
        LAST_VALUE(last_reading_read_at) OVER (ORDER BY last_reading_read_at, quality ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_reading_read_at
    FROM per_group
    WINDOW w_distinct_quality_or_unknowns AS (PARTITION BY distinct_quality_or_unknowns)
)
SELECT
    quality,
    distinct_quality_or_unknowns,
    last_reading_read_at,
    avg_voltage_vs_site_id_avg,
    n_rows,
    distinct_reading_ids,
    distinct_reading_ids_change,
    distinct_quality_or_unknowns_share_of_distinct_q
FROM sequenced
WHERE distinct_reading_ids <= 7
ORDER BY last_reading_read_at DESC NULLS FIRST, n_rows NULLS LAST
