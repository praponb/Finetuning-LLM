WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE kwh >= 35.0
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.installed_kw AS site_installed_kw
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        site_installed_kw,
        LAG(read_at) OVER (PARTITION BY site_id ORDER BY site_installed_kw ASC, id) AS prev_read_at,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        LEAD(site_installed_kw) OVER w_read_at_year_site_id_by_read AS next_site_installed_kw,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_site_id, 0), 4) AS site_installed_kw_share_of_site_id,
        RANK() OVER w_read_at_year_site_id_by_read_2 AS read_at_rank,
        LAG(kwh) OVER (PARTITION BY site_id ORDER BY site_installed_kw ASC, id) AS prev_kwh,
        kwh - LAG(kwh) OVER (PARTITION BY site_id ORDER BY site_installed_kw ASC, id) AS kwh_change,
        LAG(site_installed_kw, 3) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY site_installed_kw, id) AS prev3_site_installed_kw,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        NTH_VALUE(site_installed_kw, 2) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_installed_kw
    FROM with_parent
    WINDOW w_read_at_year_site_id_by_read AS (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id), w_site_id AS (PARTITION BY site_id), w_read_at_year_site_id_by_read_2 AS (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        site_installed_kw,
        prev_read_at,
        last_known_read_at,
        next_site_installed_kw,
        site_installed_kw_share_of_site_id,
        read_at_rank,
        prev_kwh,
        kwh_change,
        second_site_installed_kw,
        NTH_VALUE(prev3_site_installed_kw, 2) OVER (PARTITION BY read_at_rank ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev3_site_installed_kw,
        ROUND(PERCENT_RANK() OVER (PARTITION BY read_at_rank ORDER BY site_installed_kw DESC), 3) AS site_installed_kw_percent_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY read_at_rank), 2) AS read_at_rank_avg_kwh,
        COUNT(*) OVER (PARTITION BY read_at_rank ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_rank ORDER BY site_installed_kw DESC), 3) AS site_installed_kw_cume_dist,
        COUNT(*) OVER (PARTITION BY read_at_rank) AS rows_in_read_at_rank
    FROM windowed
)
-- Final projection
SELECT
    id,
    site_installed_kw,
    prev_read_at,
    next_site_installed_kw,
    read_at_rank,
    prev_kwh,
    kwh_change,
    second_site_installed_kw,
    read_at_rank_avg_kwh,
    site_installed_kw_cume_dist,
    rows_in_read_at_rank,
    FLOOR(kwh_change / 50) * 50 AS kwh_change_bucket,
    ABS(read_at_rank - 4) AS read_at_rank_distance
FROM sequenced
ORDER BY read_at_rank_distance NULLS LAST
