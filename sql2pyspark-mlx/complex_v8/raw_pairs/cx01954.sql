WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND read_at >= TIMESTAMP '2025-02-06 13:00:00'
),
joined_site AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        NTILE(3) OVER (ORDER BY read_at, id) AS read_at_tercile,
        LAG(kwh) OVER (ORDER BY read_at, id) AS prev_kwh,
        kwh - LAG(kwh) OVER (ORDER BY read_at DESC, id) AS kwh_change,
        MIN(kwh) OVER w_quality AS quality_min_kwh,
        NTH_VALUE(read_at, 2) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(AVG(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_zone, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_zone), 0), 4) AS kwh_share_of_quality_site_zone,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY read_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_read_at
    FROM joined_site
    WINDOW w_quality AS (PARTITION BY quality)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        kwh,
        site_zone,
        running_count,
        first_known_kwh,
        read_at_tercile,
        prev_kwh,
        kwh_change,
        quality_min_kwh,
        kwh_avg_2,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY DATE_TRUNC('MONTH', second_read_at), quality)) AS distinct_site_id_in_second_read_at_month_quality,
        SUM(first_known_kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', second_read_at), quality) AS second_read_at_month_quality_sum_first_known_kwh,
        SUM(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', second_read_at), quality) AS second_read_at_month_quality_sum_kwh,
        MAX(kwh_change) OVER (PARTITION BY DATE_TRUNC('MONTH', second_read_at), quality) AS second_read_at_month_quality_max_kwh_change,
        LAG(first_known_kwh, 2) OVER w_site_id_by_last_known_read_a AS prev2_first_known_kwh,
        ROUND(first_known_kwh - AVG(first_known_kwh) OVER w_second_read_at_month_quality, 2) AS first_known_kwh_vs_second_read_at_month_quality_,
        LAG(first_kwh) OVER (ORDER BY kwh_change, id) AS prev_first_kwh
    FROM with_trends
    WINDOW w_site_id_by_last_known_read_a AS (PARTITION BY site_id ORDER BY last_known_read_at DESC, id), w_second_read_at_month_quality AS (PARTITION BY DATE_TRUNC('MONTH', second_read_at), quality)
)
SELECT
    id,
    read_at,
    site_zone,
    running_count,
    first_known_kwh,
    read_at_tercile,
    prev_kwh,
    quality_min_kwh,
    kwh_avg_2,
    distinct_site_id_in_second_read_at_month_quality,
    second_read_at_month_quality_sum_kwh,
    second_read_at_month_quality_max_kwh_change,
    prev2_first_known_kwh,
    first_known_kwh_vs_second_read_at_month_quality_,
    prev_first_kwh,
    ROUND(prev_first_kwh / NULLIF(first_known_kwh, 0), 3) AS prev_first_kwh_per_first_known_kwh
FROM with_windows
WHERE running_count >= 3
ORDER BY second_read_at_month_quality_max_kwh_change
