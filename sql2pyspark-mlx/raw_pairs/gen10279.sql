WITH enriched AS (
    SELECT
        id,
        pressure,
        LEAD(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id) AS next_status_bits,
        LEAD(pressure) OVER (PARTITION BY device_code ORDER BY humidity, id) AS next_pressure,
        LAG(pressure) OVER (PARTITION BY device_code ORDER BY humidity, id) AS prev_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS event_seq,
        RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST) AS humidity_rank
    FROM device_readings
)
SELECT
    CASE WHEN humidity_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS humidity_rank_tier,
    CASE WHEN next_pressure >= 1017 THEN 'HIGH' ELSE 'LOW' END AS next_pressure_tier,
    MIN(event_seq) AS min_event_seq,
    ROUND(AVG(next_status_bits), 2) AS avg_next_status_bits,
    COUNT(*) AS num_rows
FROM enriched
GROUP BY CASE WHEN humidity_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN next_pressure >= 1017 THEN 'HIGH' ELSE 'LOW' END
ORDER BY next_pressure_tier DESC NULLS FIRST
