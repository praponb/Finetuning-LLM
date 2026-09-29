WITH valid AS (
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
    WHERE quality <> 'BAD'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.site_name
    FROM valid s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh) OVER w_read_at_quarter_read_at_mont, 2) AS read_at_quarter_read_at_month_avg_kwh,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_zone, quality_len ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        NTILE(3) OVER (ORDER BY quality_len DESC, id) AS quality_len_tercile,
        ROUND(AVG(quality_len) OVER (PARTITION BY read_at_quarter, MONTH(read_at)), 2) AS read_at_quarter_read_at_month_avg_quality_len,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY read_at_quarter)) AS distinct_quality_in_read_at_quarter,
        RANK() OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id) AS read_at_quarter_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY read_at_quarter, MONTH(read_at)), 0), 4) AS kwh_share_of_read_at_quarter_read_at_month,
        RANK() OVER (PARTITION BY quality_len, quality ORDER BY kwh DESC, id) AS kwh_rank
    FROM joined
    WINDOW w_read_at_quarter_read_at_mont AS (PARTITION BY read_at_quarter, MONTH(read_at))
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE read_at_quarter_rank <= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        voltage,
        quality,
        read_at_quarter,
        quality_len,
        site_commissioned_on,
        site_zone,
        read_at_quarter_read_at_month_avg_kwh,
        first_known_read_at,
        quality_len_tercile,
        distinct_quality_in_read_at_quarter,
        read_at_quarter_rank,
        kwh_rank,
        MAX(kwh_rank) OVER (PARTITION BY read_at_quarter_rank, read_at_quarter) AS read_at_quarter_rank_read_at_quarter_max_kwh_ran,
        ROUND(AVG(kwh_rank) OVER (ORDER BY read_at_quarter_rank, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_rank_avg_2,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_quarter_rank_2, 3) AS read_at_quarter_rank_percent_rank,
        MIN(kwh_rank) OVER (PARTITION BY read_at_quarter_rank, read_at_quarter) AS read_at_quarter_rank_read_at_quarter_min_kwh_ran
    FROM top_rows
    WINDOW w_all_by_read_at_quarter_rank_2 AS (ORDER BY read_at_quarter_rank)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        quality_len,
        quality_len_tercile,
        quality,
        ARRAY_SORT(COLLECT_SET(site_zone)) AS site_zone_list,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(read_at_quarter_rank_read_at_quarter_min_kwh_ran) AS total_read_at_quarter_rank_read_at_quarter_min_k
    FROM metrics
    GROUP BY quality_len, quality_len_tercile, quality
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        quality_len,
        quality_len_tercile,
        quality,
        site_zone_list,
        distinct_ids,
        COUNT(*) OVER (PARTITION BY quality_len, quality) AS rows_in_quality_len_quality,
        SUM(quality_len) OVER w_quality_len_quality AS quality_len_quality_sum_quality_len,
        LAST_VALUE(quality_len_tercile) OVER (ORDER BY distinct_ids ASC, quality_len, quality_len_tercile, quality ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_quality_len_tercile
    FROM stats
    WINDOW w_quality_len_quality AS (PARTITION BY quality_len, quality)
)
-- Final projection
SELECT DISTINCT quality_len, rows_in_quality_len_quality
FROM with_windows_2
ORDER BY rows_in_quality_len_quality DESC, quality_len DESC
