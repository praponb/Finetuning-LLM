WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE level = 'INFO'
),
with_windows AS (
    SELECT
        id,
        service,
        level,
        attr_key,
        flag_bit4,
        AVG(flag_bit4) OVER (PARTITION BY level ORDER BY status_code, id, attr_key ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS flag_bit4_avg_5,
        NTILE(5) OVER (PARTITION BY attr_key ORDER BY flag_bit4, id, attr_key) AS flag_bit4_bucket
    FROM flattened_attrs
),
latest AS (
    SELECT
        id,
        level,
        attr_key,
        flag_bit4,
        flag_bit4_avg_5
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, service ORDER BY flag_bit4_avg_5 DESC NULLS LAST, id, attr_key) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    SELECT
        id,
        level,
        attr_key,
        AVG(flag_bit4_avg_5) OVER (PARTITION BY attr_key ORDER BY flag_bit4, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS flag_bit4_avg_5_avg_3,
        AVG(flag_bit4) OVER (PARTITION BY attr_key, level) AS attr_key_avg_flag_bit4,
        LEAD(flag_bit4_avg_5, 2, 0) OVER (PARTITION BY attr_key, level ORDER BY flag_bit4, id, attr_key) AS next_flag_bit4_avg_5,
        PERCENT_RANK() OVER (PARTITION BY level, attr_key ORDER BY flag_bit4_avg_5 DESC NULLS LAST) AS flag_bit4_avg_5_percent_rank,
        DENSE_RANK() OVER (ORDER BY flag_bit4 DESC NULLS FIRST) AS flag_bit4_dense_rank
    FROM latest
)
SELECT
    id,
    attr_key,
    flag_bit4_avg_5_avg_3,
    attr_key_avg_flag_bit4,
    next_flag_bit4_avg_5,
    flag_bit4_avg_5_percent_rank,
    flag_bit4_dense_rank
FROM metrics
ORDER BY flag_bit4_avg_5_avg_3 NULLS LAST;
