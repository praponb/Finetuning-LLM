WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        humidity - pressure AS humidity_minus_pressure
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        humidity_minus_pressure,
        INITCAP(TRIM(device_code)) AS device_code_title,
        humidity_minus_pressure + humidity AS humidity_minus_pressure_plus_humidity
    FROM filtered
    WHERE firmware NOT IN ('2.0.1', '1.1.0')
),
base_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        temp_c,
        status_bits,
        humidity_minus_pressure,
        humidity_minus_pressure_plus_humidity,
        ROUND(humidity_minus_pressure, 2) AS humidity_minus_pressure_rounded
    FROM base
    WHERE temp_c >= 29
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(humidity_minus_pressure_plus_humidity) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS next_humidity_minus_pressure_plus_humidity,
        LEAD(humidity_minus_pressure_rounded) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS next_humidity_minus_pressure_rounded,
        LAG(humidity_minus_pressure_rounded) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS prev_humidity_minus_pressure_rounded,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS event_seq,
        MAX(humidity_minus_pressure) OVER (PARTITION BY device_code) AS device_code_max_humidity_minus_pressure
    FROM base_device_readings
    WHERE humidity_minus_pressure_rounded < 51
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_humidity_minus_pressure_rounded ASC NULLS FIRST, id ASC) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_minus_pressure_plus_humidity DESC NULLS LAST, id ASC) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    temp_c,
    humidity_minus_pressure_plus_humidity,
    humidity_minus_pressure_rounded,
    next_humidity_minus_pressure_plus_humidity,
    event_seq,
    device_code_max_humidity_minus_pressure,
    rank_in_group
FROM deduped;
