WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 10:00:00'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        quality,
        LAG(voltage) OVER w_read_at_to_date_quality_by_r AS prev_voltage,
        LEAD(voltage) OVER w_quality_site_id_by_voltage AS next_voltage,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at, id) AS read_at_dense_rank
    FROM recent_readings
    WINDOW w_read_at_to_date_quality_by_r AS (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at, id), w_quality_site_id_by_voltage AS (PARTITION BY quality, site_id ORDER BY voltage DESC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(read_at, 3) OVER (PARTITION BY quality ORDER BY prev_voltage ASC NULLS LAST, id) AS next3_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY quality ORDER BY prev_voltage ASC NULLS LAST), 3) AS prev_voltage_cume_dist,
        LEAD(read_at) OVER (PARTITION BY quality ORDER BY prev_voltage ASC NULLS LAST, id) AS next_read_at,
        COUNT(*) OVER (PARTITION BY quality ORDER BY prev_voltage ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM sequenced
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM windowed s
)
SELECT
    COALESCE(quality, 'ALL') AS quality,
    read_at_dense_rank,
    COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad,
    COUNT(DISTINCT id) AS distinct_ids
FROM profiled
GROUP BY ROLLUP(quality, read_at_dense_rank)
ORDER BY quality NULLS LAST, read_at_dense_rank NULLS LAST
