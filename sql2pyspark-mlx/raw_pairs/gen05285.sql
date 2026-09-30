WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        status_code,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM exploded
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY has_fault DESC, id, attr_key) AS rn
    FROM flattened_attrs
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status_code,
        attr_key,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY rn DESC, id, attr_key) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY has_fault DESC, id, attr_key) AS event_seq_2,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code DESC) AS status_code_cume_dist,
        RANK() OVER (ORDER BY status_code_high) AS status_code_high_rank,
        LEAD(status_code, 1) OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key) AS next_status_code
    FROM latest
)
-- Window metrics over several different partitions
SELECT
    id,
    attr_key,
    event_seq_2,
    status_code_cume_dist,
    status_code_high_rank,
    next_status_code,
    SUM(status_code_high_rank) OVER (PARTITION BY attr_key ORDER BY event_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_status_code_high_rank,
    COUNT(status_code_high_rank) OVER (PARTITION BY attr_key ORDER BY event_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_status_code_high_rank,
    MAX(status_code_high_rank) OVER (PARTITION BY attr_key ORDER BY event_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_code_high_rank
FROM metrics
