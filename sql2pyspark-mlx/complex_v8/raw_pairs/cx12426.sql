WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id <= 5
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw BETWEEN 96.9 AND 373.3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_site_id, 0), 4) AS site_installed_kw_share_of_site_id,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at) AS read_at_dense_rank,
        voltage - LAG(voltage) OVER (PARTITION BY quality, site_id ORDER BY site_installed_kw, id) AS voltage_change,
        ROUND(AVG(kwh) OVER w_site_id, 2) AS site_id_avg_kwh,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST) AS kwh_rank,
        FIRST_VALUE(kwh) OVER (PARTITION BY quality, site_id ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        site_installed_kw - LAG(site_installed_kw) OVER (PARTITION BY quality, site_id ORDER BY site_installed_kw, id) AS site_installed_kw_change,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id))) / 60, 1) AS minutes_since_prev_read_at
    FROM with_parent
    WINDOW w_site_id AS (PARTITION BY site_id)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE kwh_rank <= 3
),
stats AS (
    -- Summarise per group
    SELECT
        read_at_dense_rank,
        COUNT(*) AS n_rows,
        MIN(read_at) AS first_read_at,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        SUM(kwh_rank) AS total_kwh_rank,
        SUM(CASE WHEN quality = 'OK' THEN site_installed_kw_change ELSE 0 END) AS site_installed_kw_change_quality_ok,
        MAX(kwh_rank) AS max_kwh_rank
    FROM first_rows
    WHERE quality <> 'BAD'
    GROUP BY 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_max_kwh_rank_by_n_rows AS n_rows_quintile,
        ROUND(distinct_qualitys / NULLIF(SUM(distinct_qualitys) OVER (PARTITION BY read_at_dense_rank), 0), 4) AS distinct_qualitys_share_of_read_at_dense_rank
    FROM stats
    WINDOW w_max_kwh_rank_by_n_rows AS (PARTITION BY max_kwh_rank ORDER BY n_rows, read_at_dense_rank)
)
-- Final projection
SELECT
    read_at_dense_rank,
    first_read_at,
    distinct_qualitys,
    site_installed_kw_change_quality_ok,
    n_rows_quintile,
    distinct_qualitys_share_of_read_at_dense_rank,
    COALESCE(total_kwh_rank, 0) AS total_kwh_rank_filled,
    TO_DATE(first_read_at) AS first_read_at_date
FROM with_windows
