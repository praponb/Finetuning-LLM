WITH gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    session_id,
    CASE WHEN pressure >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_tier,
    APPROX_PERCENTILE(session_id, 0.5) AS session_id_p50,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    SUM(CASE WHEN device_code = 'DV-04' THEN is_new_session ELSE 0 END) AS dv_04_is_new_session,
    CORR(pressure, status_bits) AS corr_pressure_status_bits,
    APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles,
    PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY CORR(pressure, status_bits) DESC) AS corr_pressure_status_bits_percent_rank
FROM sessionized
GROUP BY session_id, pressure_tier;
