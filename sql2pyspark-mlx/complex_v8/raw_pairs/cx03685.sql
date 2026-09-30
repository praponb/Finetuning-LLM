WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 232.0 THEN 'HIGH' WHEN voltage >= 220.2 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
    WHERE quality IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_tier,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(AVG(voltage) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_6_prior,
        LAST_VALUE(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        COUNT(*) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        NTILE(4) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id) AS read_at_quartile,
        LEAD(kwh, 1, 0) OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id) AS next_kwh,
        MIN(kwh) OVER (PARTITION BY quality) AS quality_min_kwh
    FROM prepared
    WINDOW w_site_id AS (PARTITION BY site_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        CASE WHEN quality_min_kwh >= 720.0 THEN 'A' WHEN quality_min_kwh >= 300.0 THEN 'B' ELSE 'C' END AS quality_min_kwh_tier,
        read_at_quartile,
        SUM(kwh_max_7) AS total_kwh_max_7,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
        SUM(CASE WHEN quality = 'EST' THEN running_count ELSE 0 END) AS running_count_quality_est,
        SUM(read_at_quartile) AS total_read_at_quartile,
        ROUND(STDDEV(kwh), 2) AS stddev_kwh
    FROM windowed
    GROUP BY CASE WHEN quality_min_kwh >= 720.0 THEN 'A' WHEN quality_min_kwh >= 300.0 THEN 'B' ELSE 'C' END, read_at_quartile
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(stddev_kwh, 2) OVER (PARTITION BY read_at_quartile ORDER BY total_read_at_quartile DESC, quality_min_kwh_tier, read_at_quartile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_stddev_kwh,
        COUNT(*) OVER w_quality_min_kwh_tier_read_at AS rows_in_quality_min_kwh_tier_read_at_quartile
    FROM per_group
    WINDOW w_quality_min_kwh_tier_read_at AS (PARTITION BY quality_min_kwh_tier, read_at_quartile)
)
-- Final projection
SELECT
    quality_min_kwh_tier,
    read_at_quartile,
    total_kwh_max_7,
    running_count_quality_est,
    total_read_at_quartile,
    stddev_kwh,
    second_stddev_kwh,
    rows_in_quality_min_kwh_tier_read_at_quartile,
    SUBSTRING(TRIM(quality_min_kwh_tier), 1, 4) AS quality_min_kwh_tier_prefix,
    CASE WHEN quality_min_kwh_tier IN ('C', 'B') THEN 'group_a' WHEN quality_min_kwh_tier = 'A' THEN 'group_b' ELSE 'other' END AS quality_min_kwh_tier_group
FROM metrics
WHERE total_kwh_max_7 > 540.0
