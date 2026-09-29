WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        RANK() OVER (PARTITION BY site_id ORDER BY kwh, id) AS kwh_rank,
        DENSE_RANK() OVER w_all_by_kwh_ties AS kwh_dense_rank,
        ROW_NUMBER() OVER (ORDER BY voltage NULLS LAST, id) AS voltage_seq,
        MAX(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7_2,
        LAG(voltage) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage, id) AS prev_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY voltage, id), 3) AS voltage_percent_rank,
        ROUND(voltage_bucket - AVG(voltage_bucket) OVER (PARTITION BY site_id), 2) AS voltage_bucket_vs_site_id_avg,
        LAG(voltage_bucket) OVER w_read_at_month_by_voltage AS prev_voltage_bucket,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_read_at_month, 0), 4) AS voltage_share_of_site_id_read_at_month,
        ROW_NUMBER() OVER w_all_by_kwh AS kwh_seq
    FROM cleaned
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh ASC), w_all_by_voltage AS (ORDER BY voltage NULLS LAST, id), w_read_at_month_by_voltage AS (PARTITION BY MONTH(read_at) ORDER BY voltage, id), w_site_id_read_at_month AS (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)), w_all_by_kwh AS (ORDER BY kwh ASC, id)
)
SELECT DISTINCT kwh_seq
FROM windowed
WHERE kwh_dense_rank > 2
ORDER BY kwh_seq DESC
