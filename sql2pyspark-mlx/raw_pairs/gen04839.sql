WITH valid AS (
    SELECT
        id,
        device,
        country,
        LENGTH(device) AS device_length,
        COALESCE(country, 'N/A') AS resolved_country
    FROM sessions
    WHERE browser IS NOT DISTINCT FROM 'Safari'
      AND started_at > TO_DATE('2023-03-29')
)
SELECT
    CASE WHEN device_length >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS device_length_tier,
    MAX(device_length) AS max_device_length,
    ROUND(AVG(device_length), 2) AS avg_device_length,
    MAX(device_length) FILTER (WHERE country = 'JP') AS jp_max_device_length,
    PERCENTILE_APPROX(device_length, ARRAY(0.25, 0.5, 0.75)) AS device_length_quartiles
FROM valid
GROUP BY CUBE (CASE WHEN device_length >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY jp_max_device_length;
