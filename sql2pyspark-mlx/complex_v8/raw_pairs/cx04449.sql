WITH in_window AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        rate,
        ROUND(AVG(energy_cost) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS read_at_year_site_id_avg_energy_cost,
        SIZE(COLLECT_SET(quality) OVER w_read_at_year_site_id) AS distinct_quality_in_read_at_year_site_id,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY YEAR(read_at), site_id)) AS distinct_zone_in_read_at_year_site_id,
        ROUND(energy_cost - AVG(energy_cost) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS energy_cost_vs_read_at_year_site_id_avg
    FROM in_window
    WINDOW w_read_at_year_site_id AS (PARTITION BY YEAR(read_at), site_id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        rate,
        read_at_year_site_id_avg_energy_cost,
        distinct_quality_in_read_at_year_site_id,
        distinct_zone_in_read_at_year_site_id,
        energy_cost_vs_read_at_year_site_id_avg,
        GREATEST(rate, kwh) AS greatest_rate_kwh,
        ABS(rate - 5.7) AS rate_distance,
        CASE WHEN rate >= 5.9 THEN 'large' WHEN rate >= 4.2 THEN 'medium' ELSE 'small' END AS rate_tier
    FROM metrics
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rate >= 5.7 THEN 'A' WHEN rate >= 5.2 THEN 'B' ELSE 'C' END AS rate_tier_2,
        rate_tier,
        distinct_zone_in_read_at_year_site_id,
        SUM(greatest_rate_kwh) AS total_greatest_rate_kwh,
        PERCENTILE_APPROX(greatest_rate_kwh, 0.5) AS median_greatest_rate_kwh,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(rate_distance, 0.5) AS median_rate_distance,
        ROUND(STDDEV(rate), 2) AS stddev_rate,
        ROUND(STDDEV(distinct_quality_in_read_at_year_site_id), 2) AS stddev_distinct_quality_in_read_at_year_site_id
    FROM derived
    WHERE site_id <= 8
    GROUP BY CASE WHEN rate >= 5.7 THEN 'A' WHEN rate >= 5.2 THEN 'B' ELSE 'C' END, rate_tier, distinct_zone_in_read_at_year_site_id
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    rate_tier_2,
    distinct_zone_in_read_at_year_site_id,
    total_greatest_rate_kwh,
    median_greatest_rate_kwh,
    row_count,
    median_rate_distance,
    stddev_rate,
    stddev_distinct_quality_in_read_at_year_site_id
FROM per_group
ORDER BY median_greatest_rate_kwh DESC, row_count DESC
