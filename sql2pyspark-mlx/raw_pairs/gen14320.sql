WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(temp_c, 0) AS temp_c_rounded,
        CAST(humidity AS INT) AS humidity_int
    FROM device_readings
    WHERE device_code = 'DV-04'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        temp_c_rounded,
        humidity_int,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_int DESC, id) AS dedup_rank
    FROM sessions_device_readings
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(burst_id) OVER w_grp AS running_count_burst_id,
        SUM(burst_id) OVER w_grp AS running_sum_burst_id,
        AVG(burst_id) OVER w_grp AS running_avg_burst_id,
        NTILE(4) OVER w_ordered AS humidity_bucket
    FROM deduped
    WHERE humidity_int <> 27
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY device_code ORDER BY humidity, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_count_burst_id) OVER w_running AS running_count_burst_id_avg_5,
        LEAD(temp_c_rounded, 2) OVER (PARTITION BY device_code ORDER BY running_avg_burst_id, id) AS next_temp_c_rounded
    FROM enriched
    WINDOW w_running AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, device_code
    FROM scored
    EXCEPT
    SELECT id, device_code
    FROM device_readings
    WHERE firmware <> '1.1.0'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.id = k.id AND s.device_code = k.device_code
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    humidity_int,
    burst_id,
    dedup_rank,
    running_count_burst_id,
    running_sum_burst_id,
    CASE WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'GROUP_A' WHEN device_code RLIKE '^(DV-02|DV-03)$' THEN 'GROUP_B' ELSE 'CORE' END AS device_code_zone
FROM kept_rows
ORDER BY dedup_rank NULLS LAST
