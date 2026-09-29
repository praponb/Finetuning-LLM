WITH valid AS (
    SELECT *, ROUND(humidity, 2) AS humidity_rounded
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 05:00:00'
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST, id ASC) AS rn
    FROM valid
    QUALIFY rn = 1
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM deduped
),
flagged AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        humidity_rounded,
        rn,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
deduped_device_readings AS (
    SELECT
        id,
        device_code,
        pressure,
        firmware,
        rn,
        island_id
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY island_id DESC, id ASC) = 1
),
base AS (
    SELECT
        id,
        pressure,
        firmware,
        rn,
        island_id,
        ROUND(rn, 2) AS rn_rounded
    FROM deduped_device_readings
    WHERE pressure > 1022
),
scored AS (
    SELECT
        id,
        pressure,
        rn,
        SUM(rn_rounded) OVER w_grp AS cumulative_rn_rounded,
        SUM(rn_rounded) OVER w_grp AS running_rn_rounded
    FROM base
    WHERE firmware IN ('2.0.1', '1.0.3', '1.1.0')
    WINDOW w_grp AS (ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        SUM(cumulative_rn_rounded) OVER (ORDER BY running_rn_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_rn_rounded,
        SUM(cumulative_rn_rounded) OVER (ORDER BY pressure ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_rn_rounded,
        NTILE(4) OVER (ORDER BY pressure DESC, id ASC) AS pressure_bucket
    FROM scored
)
SELECT
    CASE WHEN cumulative_rn_rounded >= 415 THEN 'HIGH' ELSE 'LOW' END AS cumulative_rn_rounded_tier,
    CASE WHEN cumulative_cumulative_rn_rounded >= 2025 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_cumulative_rn_rounded_tier,
    CASE WHEN running_rn_rounded >= 323 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_rn_rounded_tier,
    ROUND(AVG(running_rn_rounded), 2) AS avg_running_rn_rounded,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(running_rn_rounded), 2) AS avg_running_rn_rounded_2
FROM with_windows
GROUP BY CASE WHEN cumulative_rn_rounded >= 415 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cumulative_cumulative_rn_rounded >= 2025 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_rn_rounded >= 323 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_running_rn_rounded ASC NULLS FIRST;
