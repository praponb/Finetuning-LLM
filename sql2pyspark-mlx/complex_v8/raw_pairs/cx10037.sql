WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.1, 2) AS voltage_adj,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND read_at < TIMESTAMP '2025-02-18 13:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY voltage_adj, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        ROUND(AVG(voltage_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY voltage_adj DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_adj_avg_3,
        ROUND(PERCENT_RANK() OVER w_all_by_kwh_ties, 3) AS kwh_percent_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, YEAR(read_at)), 0), 4) AS kwh_share_of_quality_read_at_year,
        FIRST_VALUE(voltage_adj) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage_adj,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, YEAR(read_at)), 2) AS kwh_vs_quality_read_at_year_avg,
        LAST_VALUE(voltage_adj) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage_adj,
        MAX(voltage_adj) OVER w_quality_read_at_year AS quality_read_at_year_max_voltage_adj,
        ROUND(AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY voltage_adj DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior
    FROM base
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh), w_quality_read_at_year AS (PARTITION BY quality, YEAR(read_at))
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(last_voltage_adj / NULLIF(kwh, 0), 3) AS last_voltage_adj_per_kwh
    FROM metrics
    WHERE kwh_share_of_quality_read_at_year BETWEEN 0.3 AND 0.8
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY kwh_share_of_quality_read_at_year) AS kwh_share_of_quality_read_at_year_dense_rank,
        ROUND(kwh_avg_6_prior - AVG(kwh_avg_6_prior) OVER (PARTITION BY site_id), 2) AS kwh_avg_6_prior_vs_site_id_avg,
        ROUND(voltage_plus_kwh / NULLIF(SUM(voltage_plus_kwh) OVER (PARTITION BY site_id), 0), 4) AS voltage_plus_kwh_share_of_site_id,
        ROW_NUMBER() OVER (ORDER BY kwh_share_of_quality_read_at_year, id) AS rn,
        COUNT(*) OVER w_site_id AS rows_in_site_id
    FROM tagged
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    voltage_plus_kwh,
    kwh_percent_rank,
    first_voltage_adj,
    last_voltage_adj,
    quality_read_at_year_max_voltage_adj,
    kwh_avg_6_prior,
    last_voltage_adj_per_kwh,
    kwh_share_of_quality_read_at_year_dense_rank,
    kwh_avg_6_prior_vs_site_id_avg,
    voltage_plus_kwh_share_of_site_id,
    rn,
    rows_in_site_id
FROM windowed
WHERE kwh_vs_quality_read_at_year_avg >= -60.0
ORDER BY quality_read_at_year_max_voltage_adj NULLS LAST, kwh_percent_rank, id
LIMIT 5
