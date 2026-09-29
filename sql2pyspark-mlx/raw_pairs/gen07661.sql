WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM lines
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM parsed
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        flag_bit2,
        new_group_flag,
        NTILE(5) OVER (PARTITION BY key ORDER BY flag_bit2, id, key) AS flag_bit2_bucket,
        CUME_DIST() OVER w_grp AS status_code_high_cume_dist,
        AVG(flag_bit2) OVER (PARTITION BY key ORDER BY new_group_flag, id, key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS flag_bit2_avg_6_prior
    FROM sessionized
    WHERE flag_bit2 > 1
    WINDOW w_grp AS (PARTITION BY key ORDER BY status_code_high DESC)
)
-- Window metrics over several different partitions
SELECT
    id,
    key,
    new_group_flag,
    flag_bit2_bucket,
    flag_bit2_avg_6_prior,
    LEAD(flag_bit2_avg_6_prior) OVER w_hist AS next_flag_bit2_avg_6_prior,
    LAG(flag_bit2) OVER w_hist AS prev_flag_bit2,
    ROW_NUMBER() OVER w_hist AS seq_no,
    STDDEV_SAMP(flag_bit2) OVER w_ordered AS key_stddev_samp_flag_bit2
FROM metrics
WINDOW w_hist AS (PARTITION BY key ORDER BY flag_bit2, id, key), w_ordered AS (PARTITION BY key);
