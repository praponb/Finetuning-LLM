WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 0.25, 2) AS voltage_adj
    FROM meter_readings
    WHERE kwh >= 45.0
      AND voltage >= 222.1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        quality,
        ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY voltage_adj, id) AS voltage_adj_seq,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage_adj, id) AS voltage_adj_rank,
        voltage_adj - LAG(voltage_adj) OVER w_all_by_kwh AS voltage_adj_change
    FROM base
    WINDOW w_all_by_kwh AS (ORDER BY kwh NULLS LAST, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, voltage_adj_rank, voltage_adj_seq
    FROM windowed
) src
UNPIVOT (
    amount FOR measure IN (voltage_adj_rank AS voltage_adj_rank_v, voltage_adj_seq AS m_voltage_adj_seq)
) unp
