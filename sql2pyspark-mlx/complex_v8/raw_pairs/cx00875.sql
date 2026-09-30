WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
    WHERE quality <> 'EST'
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_hour,
        si.tariff AS site_tariff,
        si.site_name
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff NOT IN ('FLAT', 'EV')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        quality,
        read_at_hour,
        site_tariff,
        site_name,
        LAST_VALUE(read_at_hour) OVER (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_hour,
        ROUND(PERCENT_RANK() OVER w_read_at_hour_by_read_at, 3) AS read_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST), 3) AS read_at_cume_dist,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff, read_at_hour)) AS distinct_site_id_in_site_tariff_read_at_hour,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST, id), 3) AS read_at_cume_dist_2,
        ROUND(AVG(kwh) OVER (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        SUM(read_at_hour) OVER (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS read_at_hour_sum_4,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at_hour NULLS LAST, id) AS read_at_hour_quintile,
        LEAD(read_at_hour) OVER (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST, id) AS next_read_at_hour,
        ROUND(AVG(kwh) OVER (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        NTILE(4) OVER w_read_at_hour_by_read_at AS read_at_quartile
    FROM with_details_site
    WINDOW w_read_at_hour_by_read_at AS (PARTITION BY read_at_hour ORDER BY read_at NULLS LAST, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        quality,
        read_at_hour,
        last_read_at_hour,
        read_at_percent_rank,
        distinct_site_id_in_site_tariff_read_at_hour,
        read_at_cume_dist_2,
        read_at_hour_quintile,
        next_read_at_hour,
        read_at_quartile,
        ROUND(PERCENT_RANK() OVER w_quality_distinct_site_id_in, 3) AS read_at_hour_quintile_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff ORDER BY read_at_percent_rank DESC, id), 3) AS read_at_percent_rank_cume_dist,
        LAG(read_at) OVER (PARTITION BY site_tariff ORDER BY read_at_percent_rank DESC, id) AS prev_read_at,
        FIRST_VALUE(kwh_avg_2) IGNORE NULLS OVER (PARTITION BY quality, distinct_site_id_in_site_tariff_read_at_hour ORDER BY read_at_hour_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_avg_2,
        NTILE(4) OVER (PARTITION BY quality, site_id ORDER BY read_at_percent_rank DESC, id) AS read_at_percent_rank_quartile
    FROM sequenced
    WINDOW w_quality_distinct_site_id_in AS (PARTITION BY quality, distinct_site_id_in_site_tariff_read_at_hour ORDER BY read_at_hour_quintile, id)
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    last_read_at_hour,
    read_at_percent_rank,
    distinct_site_id_in_site_tariff_read_at_hour,
    read_at_cume_dist_2,
    read_at_hour_quintile,
    next_read_at_hour,
    read_at_quartile,
    read_at_hour_quintile_percent_rank,
    read_at_percent_rank_cume_dist,
    prev_read_at,
    read_at_percent_rank_quartile,
    FLOOR(read_at_hour_quintile / 2) * 2 AS read_at_hour_quintile_bucket,
    COALESCE(read_at_percent_rank, 0.0) AS read_at_percent_rank_filled
FROM metrics
ORDER BY read_at_percent_rank_filled DESC, id
LIMIT 5
