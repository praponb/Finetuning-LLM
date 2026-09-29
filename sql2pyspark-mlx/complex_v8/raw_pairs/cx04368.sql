WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        SUM(kwh) OVER (PARTITION BY YEAR(read_at), site_id) AS read_at_year_site_id_sum_kwh,
        DENSE_RANK() OVER (ORDER BY read_at) AS read_at_dense_rank,
        RANK() OVER w_site_id_by_read_at AS read_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at), site_id ORDER BY voltage DESC, id), 3) AS voltage_cume_dist,
        ROUND(AVG(voltage) OVER (PARTITION BY TO_DATE(read_at), site_id ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS kwh_vs_read_at_year_site_id_avg
    FROM scoped_readings
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at DESC, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        read_at_year_site_id_sum_kwh,
        read_at_dense_rank,
        voltage_cume_dist,
        voltage_avg_3,
        kwh_vs_read_at_year_site_id_avg
    FROM sequenced
    WHERE read_at_rank <= 3
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    voltage_cume_dist,
    voltage_avg_3,
    kwh_vs_read_at_year_site_id_avg,
    ROUND(voltage_cume_dist * 1.07, 2) AS voltage_cume_dist_adj
FROM first_rows
WHERE kwh <= 80.8
ORDER BY id DESC NULLS FIRST
