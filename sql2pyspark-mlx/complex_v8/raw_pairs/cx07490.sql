WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE site_id >= 5
),
joined AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM valid s
    JOIN sites si ON si.id = s.site_id AND si.zone IN ('Z2', 'Z4')
    WHERE si.installed_kw >= 96.9
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4,
        NTH_VALUE(read_at, 2) OVER (ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        DENSE_RANK() OVER (ORDER BY read_at, id) AS read_at_dense_rank,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        NTH_VALUE(read_at, 2) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_2,
        MAX(site_installed_kw) OVER (ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS site_installed_kw_max_3_prior,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality
    FROM joined
    WINDOW w_quality AS (PARTITION BY quality)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    least_kwh_voltage,
    site_installed_kw,
    kwh_max_4,
    second_read_at,
    read_at_dense_rank,
    rows_in_quality,
    second_read_at_2,
    site_installed_kw_max_3_prior,
    rows_in_quality - kwh AS rows_in_quality_minus_kwh
FROM windowed
ORDER BY id DESC
