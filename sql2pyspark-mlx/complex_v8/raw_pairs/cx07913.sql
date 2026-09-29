WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'BAD')
      AND site_id IS NOT NULL
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.read_at_quarter,
        s.quality_len,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM cleaned s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw <= 373.3
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        quality_len,
        site_zone,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        COUNT(*) OVER w_site_id_read_at_quarter AS rows_in_site_id_read_at_quarter,
        LEAD(read_at) OVER (ORDER BY kwh DESC, id) AS next_read_at,
        NTH_VALUE(quality_len, 2) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY quality_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_quality_len,
        RANK() OVER (PARTITION BY site_zone ORDER BY kwh DESC, id) AS kwh_rank,
        RANK() OVER (PARTITION BY YEAR(read_at), site_id ORDER BY quality_len) AS quality_len_rank,
        SUM(voltage) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY quality_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        ROUND(CUME_DIST() OVER w_quality_len_by_voltage_ties, 3) AS voltage_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(read_at_quarter - AVG(read_at_quarter) OVER (PARTITION BY site_id), 2) AS read_at_quarter_vs_site_id_avg,
        SIZE(COLLECT_SET(site_zone) OVER w_site_id) AS distinct_site_zone_in_site_id,
        DENSE_RANK() OVER (PARTITION BY YEAR(read_at), site_id ORDER BY quality_len, id) AS quality_len_dense_rank
    FROM joined
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_read_at_quarter AS (PARTITION BY site_id, read_at_quarter), w_quality_len_by_voltage_ties AS (PARTITION BY quality_len ORDER BY voltage)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        site_zone,
        kwh_vs_site_id_avg,
        rows_in_site_id_read_at_quarter,
        next_read_at,
        second_quality_len,
        kwh_rank,
        running_voltage,
        voltage_cume_dist,
        rows_in_site_id,
        read_at_quarter_vs_site_id_avg,
        distinct_site_zone_in_site_id,
        quality_len_dense_rank,
        FIRST_VALUE(kwh_rank) OVER (ORDER BY rows_in_site_id_read_at_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_rank,
        ROUND(quality_len_rank - AVG(quality_len_rank) OVER (PARTITION BY kwh_rank), 2) AS quality_len_rank_vs_kwh_rank_avg,
        LAG(quality_len) OVER (ORDER BY kwh_rank, id) AS prev_quality_len,
        ROUND(quality_len - AVG(quality_len) OVER (PARTITION BY kwh_rank), 2) AS quality_len_vs_kwh_rank_avg,
        LEAD(rows_in_site_id) OVER (PARTITION BY quality_len_rank ORDER BY rows_in_site_id_read_at_quarter, id) AS next_rows_in_site_id,
        SUM(quality_len) OVER (PARTITION BY quality_len_rank ORDER BY rows_in_site_id_read_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quality_len,
        LAG(next_read_at) OVER w_all_by_rows_in_site_id_read_2 AS prev_next_read_at
    FROM windowed
    WINDOW w_kwh_rank AS (PARTITION BY kwh_rank), w_all_by_rows_in_site_id_read_2 AS (ORDER BY rows_in_site_id_read_at_quarter ASC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh_vs_site_id_avg,
    rows_in_site_id_read_at_quarter,
    next_read_at,
    running_voltage,
    voltage_cume_dist,
    rows_in_site_id,
    read_at_quarter_vs_site_id_avg,
    first_kwh_rank,
    quality_len_vs_kwh_rank_avg,
    next_rows_in_site_id,
    running_quality_len
FROM with_windows
WHERE running_quality_len < 106
ORDER BY running_quality_len NULLS LAST
