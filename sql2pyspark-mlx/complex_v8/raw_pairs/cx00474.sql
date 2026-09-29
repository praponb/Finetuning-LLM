WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN kwh >= 104.8 THEN 'HIGH' WHEN kwh >= 42.1 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-18'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id, quality), 0), 4) AS kwh_share_of_site_id_quality,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        SIZE(COLLECT_SET(kwh_tier) OVER (PARTITION BY site_id, quality)) AS distinct_kwh_tier_in_site_id_quality,
        ROUND(AVG(kwh) OVER (ORDER BY read_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        LAG(read_at, 2) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS prev2_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_kwh,
        MAX(kwh) OVER w_site_id_quality AS site_id_quality_max_kwh,
        RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh, id) AS kwh_rank,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh, id) AS rn,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY read_at ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        NTILE(3) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS kwh_tercile,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior
    FROM scoped_readings
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN rn >= 7 THEN 'A' WHEN rn >= 3 THEN 'B' ELSE 'C' END AS rn_tier,
        COALESCE(rows_in_site_id_quality, 0) AS rows_in_site_id_quality_filled,
        QUARTER(read_at) AS read_at_quarter
    FROM metrics
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    kwh_tier,
    kwh_share_of_site_id_quality,
    rows_in_site_id_quality,
    distinct_kwh_tier_in_site_id_quality,
    kwh_avg_3_prior,
    prev2_read_at,
    site_id_quality_avg_kwh,
    site_id_quality_max_kwh,
    kwh_rank,
    rn,
    minutes_since_prev_read_at,
    kwh_tercile,
    rn_tier,
    rows_in_site_id_quality_filled,
    read_at_quarter
FROM enriched
ORDER BY quality DESC, kwh_share_of_site_id_quality, id
LIMIT 20
