WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 83.8 THEN 'A' WHEN kwh >= 50.4 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE voltage BETWEEN 213.5 AND 235.8
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROW_NUMBER() OVER (ORDER BY read_at DESC, id) AS read_at_seq,
        SUM(voltage) OVER (PARTITION BY quality) AS quality_sum_voltage,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY voltage DESC) AS voltage_rank,
        LEAD(read_at, 2) OVER (PARTITION BY TO_DATE(read_at) ORDER BY voltage DESC, id) AS next2_read_at,
        voltage - LAG(voltage) OVER (PARTITION BY TO_DATE(read_at) ORDER BY voltage DESC, id) AS voltage_change,
        ROUND(AVG(kwh) OVER (PARTITION BY kwh_tier, site_id ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg,
        MIN(kwh) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far
    FROM scoped
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_seq,
        quality_sum_voltage,
        first_known_voltage,
        next2_read_at,
        voltage_change,
        kwh_avg_6_prior,
        voltage_vs_quality_avg,
        min_kwh_so_far
    FROM with_windows
    WHERE voltage_rank = 1
),
per_group AS (
    -- Group-level totals
    SELECT
        CASE WHEN kwh >= 82.9 THEN 'large' WHEN kwh >= 33.9 THEN 'medium' ELSE 'small' END AS kwh_tier,
        YEAR(next2_read_at) AS next2_read_at_year,
        MAX(kwh) AS max_kwh,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(next2_read_at) AS last_next2_read_at,
        ROUND(SUM(first_known_voltage) / NULLIF(SUM(kwh_avg_6_prior), 0), 3) AS first_known_voltage_to_kwh_avg_6_prior_ratio,
        PERCENTILE_APPROX(voltage_change, 0.5) AS median_voltage_change
    FROM top_rows
    GROUP BY CASE WHEN kwh >= 82.9 THEN 'large' WHEN kwh >= 33.9 THEN 'medium' ELSE 'small' END, YEAR(next2_read_at)
    HAVING SUM(voltage) > 219.6
)
SELECT
    kwh_tier,
    next2_read_at_year,
    last_next2_read_at,
    median_voltage_change,
    COALESCE(kwh_tier, 'UNKNOWN') AS kwh_tier_or_unknown,
    LENGTH(TRIM(kwh_tier)) AS kwh_tier_len
FROM per_group
ORDER BY kwh_tier_len DESC, next2_read_at_year NULLS LAST
