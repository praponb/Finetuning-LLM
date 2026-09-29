WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality_prefix,
        quality_len,
        ROUND(CUME_DIST() OVER (ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        LAG(kwh) OVER (PARTITION BY quality_len, site_id ORDER BY quality_len ASC, id) AS prev_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_cume_dist_2,
        SUM(voltage) OVER w_quality_len AS quality_len_sum_voltage,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        ROUND(quality_len / NULLIF(SUM(quality_len) OVER (PARTITION BY quality_len), 0), 4) AS quality_len_share_of_quality_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(read_at), quality_len ORDER BY quality_len), 3) AS quality_len_percent_rank,
        LAG(quality_len, 1, 0) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_quality_len
    FROM cleaned
    WINDOW w_quality_len AS (PARTITION BY quality_len)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        quality_prefix,
        quality_len,
        read_at_cume_dist_2,
        first_known_voltage,
        quality_len_percent_rank,
        ROUND(quality_len_percent_rank / NULLIF(SUM(quality_len_percent_rank) OVER w_read_at_to_date, 0), 4) AS quality_len_percent_rank_share_of_read_at_to_dat,
        MAX(read_at_cume_dist_2) OVER (PARTITION BY quality_len ORDER BY prev_quality_len, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS read_at_cume_dist_2_max_6_prior,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at)) AS rows_in_read_at_to_date,
        ROUND(prev_quality_len - AVG(prev_quality_len) OVER w_read_at_to_date, 2) AS prev_quality_len_vs_read_at_to_date_avg
    FROM with_windows
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at))
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        quality_len,
        MIN(read_at) AS first_read_at,
        ROUND(STDDEV(quality_len_percent_rank), 2) AS stddev_quality_len_percent_rank,
        COUNT(*) AS row_count,
        SUM(kwh) AS total_kwh
    FROM with_windows_2
    GROUP BY DATE_TRUNC('MONTH', read_at), quality_len
    HAVING SUM(quality_len_percent_rank_share_of_read_at_to_dat) > 0.1
)
SELECT
    read_at_month,
    quality_len,
    first_read_at,
    stddev_quality_len_percent_rank,
    row_count,
    total_kwh,
    ROUND(total_kwh * 1.1, 2) AS total_kwh_adj,
    ROUND(total_kwh * 0.25, 2) AS total_kwh_adj_2
FROM rollup_base
WHERE row_count <= 10
ORDER BY row_count DESC
