WITH recent AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND read_at < TIMESTAMP '2025-03-02 11:00:00'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        SIZE(COLLECT_SET(quality) OVER w_site_id) AS distinct_quality_in_site_id,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_quality, 0), 4) AS voltage_share_of_site_id_quality,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY kwh), 3) AS kwh_cume_dist,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality ORDER BY read_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        DENSE_RANK() OVER (ORDER BY kwh DESC) AS kwh_dense_rank,
        DENSE_RANK() OVER w_site_id_quality_by_read_at AS read_at_dense_rank,
        MAX(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_max_1_prior,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        MIN(voltage) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        SUM(voltage) OVER (PARTITION BY site_id, quality) AS site_id_quality_sum_voltage,
        MAX(voltage) OVER (PARTITION BY site_id, quality) AS site_id_quality_max_voltage
    FROM recent
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_quality AS (PARTITION BY site_id, quality), w_site_id_quality_by_read_at AS (PARTITION BY site_id, quality ORDER BY read_at ASC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(last_known_read_at) AS last_known_read_at_year,
        ROUND(kwh_dense_rank / NULLIF(voltage, 0), 3) AS kwh_dense_rank_per_voltage,
        voltage + kwh_max_1_prior AS voltage_plus_kwh_max_1_prior
    FROM metrics
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        kwh_vs_site_id_avg,
        distinct_quality_in_site_id,
        voltage_share_of_site_id_quality,
        kwh_share_of_site_id,
        kwh_cume_dist,
        kwh_dense_rank,
        read_at_dense_rank,
        kwh_max_1_prior,
        last_known_read_at,
        min_voltage_so_far,
        site_id_quality_sum_voltage,
        site_id_quality_max_voltage,
        last_known_read_at_year,
        kwh_dense_rank_per_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY distinct_quality_in_site_id), 2) AS voltage_vs_distinct_quality_in_site_id_avg,
        SUM(site_id_quality_sum_voltage) OVER (PARTITION BY quality, last_known_read_at_year ORDER BY read_at_dense_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_id_quality_sum_voltage,
        SIZE(COLLECT_SET(quality) OVER w_distinct_quality_in_site_id) AS distinct_quality_in_distinct_quality_in_site_id
    FROM annotated
    WINDOW w_distinct_quality_in_site_id AS (PARTITION BY distinct_quality_in_site_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        distinct_quality_in_distinct_quality_in_site_id,
        ROUND(STDDEV(kwh_dense_rank_per_voltage), 2) AS stddev_kwh_dense_rank_per_voltage,
        MIN(last_known_read_at) AS first_last_known_read_at,
        MAX(last_known_read_at) AS last_last_known_read_at,
        SUM(distinct_quality_in_site_id) AS total_distinct_quality_in_site_id,
        MAX(read_at) AS last_read_at
    FROM with_trends
    WHERE kwh_vs_site_id_avg > -24.0
    GROUP BY 1, 2
)
SELECT
    site_id,
    distinct_quality_in_distinct_quality_in_site_id,
    first_last_known_read_at,
    last_last_known_read_at,
    total_distinct_quality_in_site_id,
    last_read_at,
    DATEDIFF(DATE '2025-06-30', last_last_known_read_at) AS days_since_last_last_known_read_at
FROM per_group
ORDER BY site_id, distinct_quality_in_distinct_quality_in_site_id
LIMIT 20
