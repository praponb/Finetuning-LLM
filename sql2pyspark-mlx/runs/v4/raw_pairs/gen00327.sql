SELECT
    CASE WHEN temp_c >= 41.7 THEN 'LARGE' ELSE 'SMALL' END AS temp_c_tier,
    CASE WHEN humidity >= 83 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    STDDEV_SAMP(pressure) AS pressure_stddev,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    BIT_OR(status_bits) AS status_bits_bit_or,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list_2,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes
FROM device_readings
GROUP BY ROLLUP (CASE WHEN temp_c >= 41.7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN humidity >= 83 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY temp_c_tier DESC NULLS FIRST;
