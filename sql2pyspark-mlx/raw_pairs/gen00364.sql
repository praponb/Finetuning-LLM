SELECT
    CASE WHEN temp_c >= 38 THEN 'HIGH' ELSE 'LOW' END AS temp_c_tier,
    device_code,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events_2,
    MAX(pressure) AS max_pressure
FROM device_readings
GROUP BY temp_c_tier, device_code
HAVING COUNT_IF(device_code = 'DV-02') >= 1
