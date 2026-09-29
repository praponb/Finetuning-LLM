WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.5, 2) AS kwh_adj,
        CASE WHEN voltage >= 232.0 THEN 'A' WHEN voltage >= 223.3 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'EST')
      AND voltage > 222.3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        kwh_adj,
        voltage_tier,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY MONTH(read_at)), 2) AS kwh_vs_read_at_month_avg,
        LEAD(voltage, 3) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS next3_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY MONTH(read_at)), 0), 4) AS kwh_share_of_read_at_month,
        LAG(kwh_adj, 1, 0) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS prev_kwh_adj,
        LAG(kwh, 1, 0) OVER w_all_by_kwh AS prev_kwh,
        LAST_VALUE(kwh) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh
    FROM scoped_readings
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC NULLS FIRST, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, next3_voltage, kwh_adj, kwh_share_of_read_at_month, prev_kwh
    FROM with_windows
) src
UNPIVOT (
    amount FOR metric IN (next3_voltage AS next3_voltage_v, kwh_adj AS m_kwh_adj, kwh_share_of_read_at_month AS kwh_share_of_read_at_month_v, prev_kwh AS prev_kwh_v)
) unp
ORDER BY id, metric
