WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CONCAT(device_code, ' ', firmware) AS device_code_firmware,
        CONCAT_WS(',', firmware, device_code) AS firmware_device_code_key
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(humidity) OVER (ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS humidity_avg_6,
        RANK() OVER (PARTITION BY device_code ORDER BY humidity NULLS FIRST) AS humidity_rank,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS event_seq
    FROM filtered
    WHERE humidity <= 50
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_rank DESC NULLS LAST, id) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
),
latest_device_readings AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        humidity,
        pressure,
        firmware,
        firmware_device_code_key,
        humidity_avg_6,
        humidity_rank,
        rank_in_group,
        rn
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS latest_rank
        FROM deduped
    ) t
    WHERE latest_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN humidity >= 75 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    CASE WHEN pressure >= 1020 THEN 'LARGE' ELSE 'SMALL' END AS pressure_tier,
    CASE WHEN humidity_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS humidity_rank_tier,
    MAX(humidity) AS max_humidity,
    COUNT(*) AS cnt
FROM latest_device_readings
GROUP BY humidity_tier, pressure_tier, humidity_rank_tier
ORDER BY pressure_tier;
