WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        kwh,
        voltage,
        quality,
        quality_or_unknown,
        voltage_filled,
        RANK() OVER (PARTITION BY site_id ORDER BY kwh) AS kwh_rank,
        ROUND(CUME_DIST() OVER w_site_id_by_kwh, 3) AS kwh_cume_dist,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        SUM(kwh) OVER (PARTITION BY quality_or_unknown, site_id) AS quality_or_unknown_site_id_sum_kwh
    FROM filtered_readings
    WINDOW w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        quality,
        quality_or_unknown,
        kwh_rank,
        kwh_cume_dist,
        second_read_at,
        quality_or_unknown_site_id_sum_kwh,
        CONCAT(quality, '#', CAST(kwh_rank AS STRING)) AS quality_ref,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        DATE_TRUNC('QUARTER', second_read_at) AS second_read_at_quarter
    FROM sequenced
    WHERE quality_or_unknown = 'UNKNOWN'
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        quality,
        quality_or_unknown,
        kwh_cume_dist,
        second_read_at,
        quality_or_unknown_site_id_sum_kwh,
        quality_ref,
        kwh_bucket,
        second_read_at_quarter
    FROM annotated
    WHERE kwh_rank <= 2
      AND second_read_at_quarter >= TIMESTAMP '2025-01-13 09:00:00'
)
SELECT
    id,
    kwh_cume_dist,
    second_read_at,
    quality_or_unknown_site_id_sum_kwh,
    quality_ref,
    kwh_bucket,
    CONCAT_WS(': ', quality_or_unknown, quality) AS quality_or_unknown_quality_label,
    ROUND(kwh_bucket / NULLIF(kwh_cume_dist, 0), 3) AS kwh_bucket_per_kwh_cume_dist
FROM top_per_group
WHERE kwh_cume_dist <= 0.6
ORDER BY kwh_bucket_per_kwh_cume_dist DESC, quality_ref DESC
