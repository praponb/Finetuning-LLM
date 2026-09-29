WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        status_code,
        k,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM flattened
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code_high, id, k) AS latest_rank
    FROM flattened_attrs
    QUALIFY latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host, k ORDER BY latest_rank, id, k) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        status_code,
        k,
        attr_value,
        status_code_high,
        flag_bit1,
        rank_in_group,
        rank_in_group - flag_bit1 AS rank_in_group_minus_flag_bit1
    FROM deduped
    WHERE flag_bit1 = 1
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY host, k), 4) AS status_code_share,
    LAG(flag_bit1, 1) OVER (PARTITION BY host ORDER BY status_code, id, k) AS prev_flag_bit1,
    MAX(status_code_high) OVER (PARTITION BY host) AS host_max_status_code_high
FROM base;
