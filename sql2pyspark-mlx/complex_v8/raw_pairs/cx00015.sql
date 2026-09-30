WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 11
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        COUNT(*) OVER w_read_at_to_date_quality AS rows_in_read_at_to_date_quality,
        LAST_VALUE(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC NULLS FIRST), 3) AS read_at_percent_rank,
        ROUND(CUME_DIST() OVER w_quality_by_read_at, 3) AS read_at_cume_dist,
        MAX(kwh) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_max_3,
        NTILE(3) OVER (ORDER BY kwh DESC, id) AS kwh_tercile,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at), quality) AS read_at_to_date_quality_sum_kwh,
        ROUND(AVG(kwh) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality
    FROM recent
    WINDOW w_read_at_to_date_quality AS (PARTITION BY TO_DATE(read_at), quality), w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT *, ABS(kwh_tercile - 4) AS kwh_tercile_distance
    FROM windowed
)
-- Final projection
SELECT
    id,
    read_at,
    last_kwh,
    read_at_percent_rank,
    read_at_cume_dist,
    kwh_tercile,
    read_at_to_date_quality_sum_kwh,
    kwh_avg_2,
    ROUND(kwh_avg_2 / NULLIF(read_at_cume_dist, 0), 3) AS kwh_avg_2_per_read_at_cume_dist,
    QUARTER(read_at) AS read_at_quarter
FROM scored
ORDER BY read_at_to_date_quality_sum_kwh DESC, last_kwh
