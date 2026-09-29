WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 106.0
      AND voltage >= 216.1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank,
        RANK() OVER (ORDER BY voltage ASC) AS voltage_rank,
        SUM(voltage) OVER w_quality_site_id AS quality_site_id_sum_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS voltage_vs_quality_site_id_avg,
        LAG(kwh) OVER (ORDER BY voltage ASC, id) AS prev_kwh,
        NTILE(5) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS kwh_quintile,
        NTILE(10) OVER (ORDER BY kwh, id) AS kwh_decile,
        NTILE(4) OVER (ORDER BY kwh, id) AS kwh_quartile
    FROM scoped_readings
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh DESC NULLS FIRST), w_quality_site_id AS (PARTITION BY quality, site_id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE kwh_rank = 1
      AND kwh_quintile <= 6
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        kwh_rank,
        site_id,
        ROUND(SUM(quality_site_id_sum_voltage), 2) AS total_quality_site_id_sum_voltage,
        SUM(kwh_decile) AS total_kwh_decile
    FROM first_rows
    GROUP BY kwh_rank, site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_kwh_decile NULLS LAST, kwh_rank, site_id) AS seq_no,
        ROUND(AVG(total_kwh_decile) OVER (PARTITION BY site_id ORDER BY total_quality_site_id_sum_voltage DESC, kwh_rank, site_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS total_kwh_decile_avg_7,
        LAST_VALUE(total_kwh_decile) IGNORE NULLS OVER (ORDER BY total_kwh_decile NULLS LAST, kwh_rank, site_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_kwh_decile
    FROM totals
)
-- Final projection
SELECT
    kwh_rank,
    total_quality_site_id_sum_voltage,
    total_kwh_decile,
    total_kwh_decile_avg_7,
    COALESCE(kwh_rank, 0) AS kwh_rank_filled
FROM windowed
ORDER BY total_kwh_decile, total_quality_site_id_sum_voltage DESC NULLS FIRST
