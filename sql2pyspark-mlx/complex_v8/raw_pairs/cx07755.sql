WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 109.0 THEN 'A' WHEN kwh >= 33.9 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        quality,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        ROUND(AVG(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY voltage ASC, id) AS voltage_rank,
        MIN(voltage) OVER (PARTITION BY kwh_tier) AS kwh_tier_min_voltage,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        voltage - LAG(voltage) OVER (ORDER BY voltage ASC, id) AS voltage_change,
        SUM(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        NTILE(10) OVER (ORDER BY voltage ASC, id) AS voltage_decile,
        MAX(voltage) OVER (ORDER BY voltage ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS voltage_max_2,
        ROUND(PERCENT_RANK() OVER w_quality_site_id_by_voltage, 3) AS voltage_percent_rank,
        LEAD(kwh, 2, 0) OVER w_all_by_voltage_2 AS next2_kwh,
        voltage - LAG(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage ASC, id) AS voltage_change_2,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS voltage_rank_2,
        MAX(voltage) OVER (PARTITION BY kwh_tier) AS kwh_tier_max_voltage,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality, site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage
    FROM base
    WINDOW w_quality_site_id_by_voltage AS (PARTITION BY quality, site_id ORDER BY voltage ASC, id), w_all_by_voltage_2 AS (ORDER BY voltage DESC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ABS(running_kwh - 600.0) AS running_kwh_distance,
        CASE WHEN voltage_change_2 >= 73.5 THEN 'A' WHEN voltage_change_2 >= -122.5 THEN 'B' ELSE 'C' END AS voltage_change_2_tier,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM with_windows
),
per_group AS (
    -- Group-level totals
    SELECT
        voltage_rank_2,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        SUM(CASE WHEN quality IN ('EST', 'OK') THEN 1 ELSE 0 END) AS n_quality_in_est_ok,
        COUNT(*) AS n_rows
    FROM annotated
    GROUP BY 1
)
SELECT
    voltage_rank_2,
    distinct_qualitys,
    n_quality_in_est_ok,
    n_rows,
    GREATEST(n_rows, n_quality_in_est_ok) AS greatest_n_rows_n_quality_in_est_ok,
    ABS(distinct_qualitys - 4) AS distinct_qualitys_distance
FROM per_group
WHERE n_rows > 2
ORDER BY greatest_n_rows_n_quality_in_est_ok, voltage_rank_2 DESC NULLS FIRST
