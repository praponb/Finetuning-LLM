WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE voltage > 223.3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(voltage_minus_kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_voltage_minus_kwh,
        LAG(kwh) OVER w_all_by_kwh AS prev_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_id), 0), 4) AS kwh_share_of_quality_site_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh), 3) AS kwh_percent_rank,
        SUM(voltage_minus_kwh) OVER (ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_minus_kwh_sum_7
    FROM scoped
    WINDOW w_all_by_kwh AS (ORDER BY kwh, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh_percent_rank) OVER (PARTITION BY quality), 2) AS quality_avg_kwh_percent_rank,
        FIRST_VALUE(voltage_minus_kwh) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_minus_kwh,
        ROUND(CUME_DIST() OVER (ORDER BY quality_site_id_min_voltage_minus_kwh, id), 3) AS quality_site_id_min_voltage_minus_kwh_cume_dist
    FROM sequenced
)
-- Subtotals with ROLLUP
SELECT
    site_id,
    quality,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM metrics
GROUP BY ROLLUP(site_id, quality)
ORDER BY site_id NULLS LAST, quality NULLS LAST
