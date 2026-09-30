WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(status_code_high) OVER (PARTITION BY k ORDER BY status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_high,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_dense_rank,
        FIRST_VALUE(status_code_high, TRUE) OVER (ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_high,
        SUM(flag_bit2) OVER w_hist AS running_flag_bit2
    FROM unpacked
    WHERE host IN ('h4', 'h1')
    WINDOW w_hist AS (PARTITION BY k, host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        status_code_high * running_flag_bit2 AS status_code_high_times_running_flag_bit2,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM enriched
    WHERE k IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, k) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value,
        status_code_high,
        running_status_code_high,
        status_code_high_times_running_flag_bit2,
        k_filled,
        SUM(new_group_flag) OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    host,
    log_ts,
    k,
    attr_value,
    status_code_high,
    running_status_code_high,
    status_code_high_times_running_flag_bit2
FROM islands
ORDER BY host DESC;
