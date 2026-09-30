WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter
    FROM meter_readings
    WHERE voltage >= 222.3
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.read_at_quarter,
        si.commissioned_on AS site_commissioned_on
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        quality,
        site_commissioned_on,
        ROUND(CUME_DIST() OVER w_read_at_quarter_by_site_comm, 3) AS site_commissioned_on_cume_dist,
        ROUND(read_at_quarter / NULLIF(SUM(read_at_quarter) OVER (PARTITION BY quality), 0), 4) AS read_at_quarter_share_of_quality,
        RANK() OVER (PARTITION BY read_at_quarter ORDER BY site_commissioned_on DESC) AS site_commissioned_on_rank,
        LEAD(read_at_quarter, 2) OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id) AS next2_read_at_quarter,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        LAST_VALUE(read_at_quarter) IGNORE NULLS OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at_quarter,
        ROUND(AVG(kwh) OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        COUNT(*) OVER w_quality AS rows_in_quality,
        LEAD(read_at_quarter, 1, 0) OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id) AS next_read_at_quarter,
        MAX(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_max_6_prior,
        LEAD(read_at_quarter) OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id) AS next_read_at_quarter_2
    FROM enriched
    WINDOW w_read_at_quarter_by_site_comm AS (PARTITION BY read_at_quarter ORDER BY site_commissioned_on DESC), w_quality AS (PARTITION BY quality)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        site_commissioned_on,
        site_commissioned_on_cume_dist,
        read_at_quarter_share_of_quality,
        site_commissioned_on_rank,
        last_known_read_at_quarter,
        rows_in_quality,
        next_read_at_quarter,
        kwh_max_6_prior,
        next_read_at_quarter_2,
        SIZE(COLLECT_SET(site_id) OVER w_last_known_read_at_quarter_s) AS distinct_site_id_in_last_known_read_at_quarter_s,
        NTH_VALUE(site_commissioned_on, 2) OVER (PARTITION BY site_id ORDER BY rows_in_quality, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        SUM(next_read_at_quarter_2) OVER (PARTITION BY last_known_read_at_quarter, YEAR(site_commissioned_on)) AS last_known_read_at_quarter_site_commissioned_on_
    FROM windowed
    WINDOW w_last_known_read_at_quarter_s AS (PARTITION BY last_known_read_at_quarter, YEAR(site_commissioned_on))
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(site_commissioned_on, 'yyyy-MM') AS site_commissioned_on_ym,
        site_commissioned_on_rank,
        MIN(site_commissioned_on) AS first_site_commissioned_on,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(*) AS cnt
    FROM sequenced
    GROUP BY DATE_FORMAT(site_commissioned_on, 'yyyy-MM'), site_commissioned_on_rank
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    site_commissioned_on_ym,
    site_commissioned_on_rank,
    first_site_commissioned_on,
    distinct_site_ids,
    cnt
FROM per_group
ORDER BY cnt DESC
