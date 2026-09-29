WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CAST(status_bits AS DOUBLE) AS status_bits_str
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND device_code <> 'DV-04'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_str ASC NULLS FIRST, id ASC) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        firmware,
        status_bits_str,
        LEAD(temp_c) OVER w_hist AS next_temp_c,
        LAG(humidity) OVER w_hist AS prev_humidity,
        LEAD(humidity) OVER w_hist AS next_humidity,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(reading_ts), device_code ORDER BY reading_ts DESC, id ASC) AS seq_no
    FROM deduped
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY status_bits_str ASC, id ASC)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) AS rn
    FROM enriched
    QUALIFY rn = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_humidity ASC, id ASC) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN prev_humidity >= 81 THEN 'LARGE' ELSE 'SMALL' END AS prev_humidity_tier,
    device_code,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN prev_humidity >= 81 THEN 'LARGE' ELSE 'SMALL' END, device_code), (CASE WHEN prev_humidity >= 81 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY distinct_firmwares ASC NULLS LAST
