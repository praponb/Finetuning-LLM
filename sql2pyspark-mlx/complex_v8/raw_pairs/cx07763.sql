WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage <= 233.1
      AND quality = 'OK'
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.days_since_read_at,
        si.zone AS site_zone
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z3'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_zone ORDER BY days_since_read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(days_since_read_at - AVG(days_since_read_at) OVER (PARTITION BY quality), 2) AS days_since_read_at_vs_quality_avg,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY quality)) AS distinct_site_zone_in_quality,
        ROUND(CUME_DIST() OVER (PARTITION BY site_zone, TO_DATE(read_at) ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        MAX(days_since_read_at) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY kwh NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_read_at_max_4,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(days_since_read_at / NULLIF(SUM(days_since_read_at) OVER (PARTITION BY quality), 0), 4) AS days_since_read_at_share_of_quality,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        SUM(kwh) OVER w_quality AS quality_sum_kwh,
        ROW_NUMBER() OVER w_site_id_read_at_month_by_kwh_2 AS rn,
        RANK() OVER w_site_id_read_at_month_by_kwh_2 AS kwh_rank,
        ROUND(AVG(days_since_read_at) OVER w_quality, 2) AS quality_avg_days_since_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY quality), 2) AS quality_avg_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY kwh NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2
    FROM with_parent_site
    WINDOW w_quality AS (PARTITION BY quality), w_site_id_read_at_month_by_kwh_2 AS (PARTITION BY site_id, MONTH(read_at) ORDER BY kwh NULLS LAST, id)
)
SELECT
    id,
    site_id,
    kwh,
    site_zone,
    second_kwh,
    days_since_read_at_vs_quality_avg,
    distinct_site_zone_in_quality,
    read_at_cume_dist,
    days_since_read_at_max_4,
    rows_in_quality,
    days_since_read_at_share_of_quality,
    kwh_vs_quality_avg,
    quality_sum_kwh,
    rn,
    kwh_rank,
    quality_avg_kwh,
    kwh_avg_2,
    FLOOR(days_since_read_at / 100) * 100 AS days_since_read_at_bucket
FROM with_trends
ORDER BY kwh_avg_2 DESC, days_since_read_at_vs_quality_avg NULLS LAST
