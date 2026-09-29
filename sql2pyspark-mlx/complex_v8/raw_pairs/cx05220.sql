WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage >= 215.6
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        voltage,
        quality,
        quality_len,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM prepared_readings
)
SELECT
    site_id,
    quality_len,
    COUNT(DISTINCT site_id) AS distinct_site_ids,
    COUNT(*) AS cnt,
    ROUND(100 * COUNT(CASE WHEN quality = 'EST' THEN 1 END) / COUNT(*), 1) AS pct_quality_est
FROM scored
GROUP BY CUBE(site_id, quality_len)
ORDER BY site_id NULLS LAST, quality_len NULLS LAST
