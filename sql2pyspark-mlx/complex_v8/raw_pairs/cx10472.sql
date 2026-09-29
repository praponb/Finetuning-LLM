WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
      AND site_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        quality,
        read_at_week,
        SUM(days_since_read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_week), DATE_TRUNC('MONTH', read_at) ORDER BY read_at_week, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_read_at_sum_2_prior,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY kwh DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        COUNT(*) OVER (PARTITION BY MONTH(read_at), quality) AS rows_in_read_at_month_quality,
        ROW_NUMBER() OVER (PARTITION BY site_id, YEAR(read_at_week) ORDER BY days_since_read_at DESC, id) AS days_since_read_at_seq,
        ROUND(days_since_read_at - AVG(days_since_read_at) OVER (PARTITION BY MONTH(read_at), quality), 2) AS days_since_read_at_vs_read_at_month_quality_avg,
        COUNT(*) OVER (PARTITION BY YEAR(read_at_week), quality) AS rows_in_read_at_week_year_quality,
        ROUND(days_since_read_at / NULLIF(SUM(days_since_read_at) OVER (PARTITION BY YEAR(read_at_week), quality), 0), 4) AS days_since_read_at_share_of_read_at_week_year_qu,
        FIRST_VALUE(read_at_week) IGNORE NULLS OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at_week,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY days_since_read_at), 3) AS days_since_read_at_percent_rank,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at ASC, id) AS read_at_seq,
        kwh - LAG(kwh) OVER w_quality_by_days_since_read_a_2 AS kwh_change,
        RANK() OVER (PARTITION BY quality ORDER BY days_since_read_at, id) AS days_since_read_at_rank,
        MAX(kwh) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7
    FROM cleaned
    WINDOW w_site_id_read_at_week_year_by AS (PARTITION BY site_id, YEAR(read_at_week) ORDER BY days_since_read_at DESC, id), w_quality_by_days_since_read_a_2 AS (PARTITION BY quality ORDER BY days_since_read_at, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at_week,
        minutes_since_prev_read_at,
        rows_in_read_at_month_quality,
        days_since_read_at_seq,
        days_since_read_at_vs_read_at_month_quality_avg,
        days_since_read_at_share_of_read_at_week_year_qu,
        days_since_read_at_percent_rank,
        kwh_sum_1_prior,
        kwh_change,
        days_since_read_at_rank,
        kwh_max_7,
        FIRST_VALUE(kwh_change) OVER (PARTITION BY site_id, YEAR(read_at_week) ORDER BY days_since_read_at_percent_rank ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_change,
        MIN(days_since_read_at_rank) OVER w_site_id_quality AS site_id_quality_min_days_since_read_at_rank,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality,
        ROW_NUMBER() OVER (PARTITION BY days_since_read_at_rank, site_id ORDER BY kwh_sum_1_prior ASC, id) AS kwh_sum_1_prior_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY days_since_read_at_rank, site_id ORDER BY kwh_sum_1_prior ASC), 3) AS kwh_sum_1_prior_cume_dist,
        ROUND((UNIX_TIMESTAMP(read_at_week) - UNIX_TIMESTAMP(LAG(read_at_week) OVER (PARTITION BY site_id, YEAR(read_at_week) ORDER BY days_since_read_at_percent_rank ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_read_at_week
    FROM with_windows
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at_week,
    minutes_since_prev_read_at,
    days_since_read_at_seq,
    days_since_read_at_vs_read_at_month_quality_avg,
    days_since_read_at_share_of_read_at_week_year_qu,
    days_since_read_at_percent_rank,
    kwh_sum_1_prior,
    kwh_change,
    first_kwh_change,
    site_id_quality_min_days_since_read_at_rank,
    rows_in_site_id_quality,
    kwh_sum_1_prior_seq,
    minutes_since_prev_read_at_week,
    kwh_sum_1_prior_seq + rows_in_site_id_quality AS kwh_sum_1_prior_seq_plus_rows_in_site_id_quality,
    LEAST(days_since_read_at_seq, kwh_change) AS least_days_since_read_at_seq_kwh_change
FROM sequenced
WHERE site_id_quality_min_days_since_read_at_rank BETWEEN 31 AND 95
ORDER BY kwh_sum_1_prior_seq, days_since_read_at_vs_read_at_month_quality_avg
