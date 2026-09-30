WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        least_voltage_kwh,
        ROUND(least_voltage_kwh / NULLIF(SUM(least_voltage_kwh) OVER (PARTITION BY quality, site_id), 0), 4) AS least_voltage_kwh_share_of_quality_site_id,
        SUM(least_voltage_kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS least_voltage_kwh_sum_2,
        ROUND(AVG(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior
    FROM prepared
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    SUM(CASE WHEN quality IN ('BAD', 'OK') THEN least_voltage_kwh_share_of_quality_site_id ELSE 0 END) AS least_voltage_kwh_share_of_quality_site_id_quali,
    SUM(least_voltage_kwh) FILTER (WHERE quality = 'EST') AS least_voltage_kwh_quality_est,
    MIN(kwh_avg_6_prior) AS min_kwh_avg_6_prior
FROM with_trends
GROUP BY GROUPING SETS ((quality, site_id), (quality), ())
ORDER BY quality NULLS LAST, site_id NULLS LAST
