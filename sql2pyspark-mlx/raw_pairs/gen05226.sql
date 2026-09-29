SELECT
    customer_id,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT device) AS distinct_devices,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count
FROM sessions
GROUP BY customer_id
ORDER BY num_rows NULLS FIRST
