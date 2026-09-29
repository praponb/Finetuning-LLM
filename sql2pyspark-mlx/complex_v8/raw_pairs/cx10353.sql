WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        ROUND(kwh * 1.07, 2) AS kwh_adj,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE site_id >= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_voltage)) / 60, 1) AS minutes_since_prev_read_at,
        LEAD(read_at) OVER w_site_id_read_at_month_by_rea AS next_read_at,
        NTILE(10) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id) AS read_at_decile,
        ROUND(AVG(kwh) OVER (ORDER BY voltage ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3
    FROM prepared_readings
    WINDOW w_all_by_voltage AS (ORDER BY voltage ASC, id), w_site_id_read_at_month_by_rea AS (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id)
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    read_at_decile,
    ROUND(SUM(minutes_since_prev_read_at) / NULLIF(SUM(kwh_filled), 0), 3) AS minutes_since_prev_read_at_to_kwh_filled_ratio
FROM windowed
GROUP BY GROUPING SETS ((site_id, read_at_decile), (site_id))
ORDER BY site_id NULLS LAST, read_at_decile NULLS LAST
