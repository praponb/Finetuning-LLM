WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.5, 2) AS voltage_adj,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE voltage BETWEEN 215.7 AND 240.9
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage_adj,
        days_since_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY MONTH(read_at)), 2) AS kwh_vs_read_at_month_avg,
        LAG(read_at) OVER (ORDER BY days_since_read_at DESC NULLS FIRST, id) AS prev_read_at
    FROM prepared
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage_adj,
        days_since_read_at,
        kwh_vs_read_at_month_avg,
        prev_read_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage_adj DESC), 3) AS voltage_adj_percent_rank,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        MAX(kwh) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_max_3,
        NTH_VALUE(voltage_adj, 2) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_adj
    FROM qualified
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND MONTH(m.read_at) IN (2, 3)
              AND m.voltage < 233.6
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(kwh_vs_read_at_month_avg) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY second_voltage_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_vs_read_at_month_avg,
        NTILE(10) OVER (ORDER BY second_voltage_adj DESC, id) AS second_voltage_adj_decile,
        NTILE(4) OVER (ORDER BY second_voltage_adj DESC, id) AS second_voltage_adj_quartile
    FROM with_activity
)
SELECT
    id,
    site_id,
    read_at,
    voltage_adj,
    days_since_read_at,
    kwh_vs_read_at_month_avg,
    prev_read_at,
    voltage_adj_percent_rank,
    rows_in_site_id,
    kwh_max_3,
    second_voltage_adj,
    last_known_kwh_vs_read_at_month_avg,
    second_voltage_adj_decile,
    DATEDIFF(DATE '2025-12-31', prev_read_at) AS days_since_prev_read_at,
    ROUND(rows_in_site_id / NULLIF(second_voltage_adj, 0), 3) AS rows_in_site_id_per_second_voltage_adj
FROM windowed
WHERE kwh_max_3 < 234.0
ORDER BY read_at NULLS LAST
