WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 106.5 THEN 'HIGH' WHEN kwh >= 45.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        CASE WHEN voltage >= 232.4 THEN 'HIGH' WHEN voltage >= 222.1 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_tier,
        SUM(kwh) OVER (PARTITION BY kwh_tier) AS kwh_tier_sum_kwh,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        kwh - LAG(kwh) OVER (PARTITION BY YEAR(read_at), kwh_tier ORDER BY kwh DESC, id) AS kwh_change,
        NTH_VALUE(kwh, 2) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh,
        SIZE(COLLECT_SET(quality) OVER w_site_id) AS distinct_quality_in_site_id,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        LAG(read_at, 3) OVER (ORDER BY kwh DESC, id) AS prev3_read_at,
        LAST_VALUE(read_at) OVER (PARTITION BY YEAR(read_at), kwh_tier ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        MAX(kwh) OVER (PARTITION BY quality) AS quality_max_kwh,
        LAG(voltage, 1, 0) OVER (ORDER BY read_at DESC, id) AS prev_voltage
    FROM scoped
    WINDOW w_site_id AS (PARTITION BY site_id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_tier,
        kwh_tier_sum_kwh,
        rows_in_quality,
        kwh_change,
        second_kwh,
        site_id_max_kwh,
        distinct_quality_in_site_id,
        kwh_share_of_site_id,
        prev3_read_at,
        last_read_at,
        quality_max_kwh,
        ROUND(AVG(quality_max_kwh) OVER (PARTITION BY distinct_quality_in_site_id, TO_DATE(last_read_at)), 2) AS distinct_quality_in_site_id_last_read_at_to_date,
        MIN(kwh_tier_sum_kwh) OVER (PARTITION BY distinct_quality_in_site_id, TO_DATE(last_read_at)) AS distinct_quality_in_site_id_last_read_at_to_date_2,
        ROUND(PERCENT_RANK() OVER w_site_id_by_read_at_ties, 3) AS read_at_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY last_read_at), 3) AS last_read_at_cume_dist,
        LEAD(rows_in_quality) OVER w_all_by_last_read_at AS next_rows_in_quality,
        kwh_tier_sum_kwh - LAG(kwh_tier_sum_kwh) OVER (ORDER BY last_read_at, id) AS kwh_tier_sum_kwh_change
    FROM with_trends
    WINDOW w_site_id_by_read_at_ties AS (PARTITION BY site_id ORDER BY read_at), w_all_by_last_read_at AS (ORDER BY last_read_at, id)
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    quality,
    voltage_tier,
    kwh_tier_sum_kwh,
    second_kwh,
    site_id_max_kwh,
    distinct_quality_in_site_id,
    kwh_share_of_site_id,
    last_read_at,
    quality_max_kwh,
    distinct_quality_in_site_id_last_read_at_to_date,
    read_at_percent_rank,
    last_read_at_cume_dist,
    next_rows_in_quality,
    DATE_TRUNC('WEEK', read_at) AS read_at_week,
    CASE WHEN voltage_tier IN ('MEDIUM', 'LOW') THEN 'group_a' WHEN voltage_tier = 'HIGH' THEN 'group_b' ELSE 'other' END AS voltage_tier_group
FROM sequenced
ORDER BY site_id_max_kwh NULLS LAST
