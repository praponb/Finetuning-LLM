WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        YEAR(read_at) AS read_at_year,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE (voltage > 215.7 OR quality = 'BAD')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at_year,
        read_at_label,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at_year), 3) AS read_at_year_percent_rank,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at), read_at_year ORDER BY kwh, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_sum_4,
        NTILE(3) OVER w_all_by_read_at_year AS read_at_year_tercile,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, read_at_year), 2) AS quality_read_at_year_avg_kwh
    FROM base_readings
    WINDOW w_all_by_read_at_year AS (ORDER BY read_at_year, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(read_at_year_tercile, 0) AS read_at_year_tercile_filled,
        LOWER(REGEXP_REPLACE(TRIM(read_at_label), ' ', '_')) AS read_at_label_slug
    FROM windowed
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM labeled
    WHERE read_at_year_tercile < (SELECT MAX(read_at_year_tercile) FROM labeled)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY read_at_year, read_at_year_tercile) AS rows_in_read_at_year_read_at_year_tercile,
        ROUND(CUME_DIST() OVER w_read_at_year_by_read_at_year, 3) AS read_at_year_percent_rank_cume_dist,
        ROUND(AVG(read_at_year_tercile) OVER (PARTITION BY read_at_year ORDER BY read_at_year_percent_rank DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS read_at_year_tercile_avg_2_prior,
        NTILE(5) OVER (PARTITION BY read_at_year ORDER BY read_at_year_percent_rank DESC, id) AS read_at_year_percent_rank_quintile,
        NTH_VALUE(read_at_year_tercile_filled, 2) OVER (PARTITION BY read_at_year ORDER BY read_at_year_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_year_tercile_filled
    FROM qualified
    WINDOW w_read_at_year_by_read_at_year AS (PARTITION BY read_at_year ORDER BY read_at_year_percent_rank DESC)
)
-- Final projection
SELECT
    id,
    read_at_label,
    read_at_year_percent_rank,
    kwh_sum_4,
    read_at_year_tercile,
    quality_read_at_year_avg_kwh,
    read_at_year_tercile_filled,
    read_at_label_slug,
    rows_in_read_at_year_read_at_year_tercile,
    read_at_year_percent_rank_cume_dist,
    read_at_year_tercile_avg_2_prior,
    read_at_year_percent_rank_quintile,
    second_read_at_year_tercile_filled,
    LEAST(kwh_sum_4, read_at_year) AS least_kwh_sum_4_read_at_year
FROM with_windows
