WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        level,
        k,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        k,
        attr_value,
        flag_bit8,
        AVG(flag_bit8) OVER (PARTITION BY level ORDER BY flag_bit8 ASC, id ASC, k ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS flag_bit8_avg_5,
        RANK() OVER w_running AS status_code_high_rank
    FROM flattened_attrs
    WINDOW w_running AS (PARTITION BY level, k ORDER BY status_code_high ASC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        k,
        attr_value,
        flag_bit8,
        flag_bit8_avg_5,
        MAX(status_code_high_rank) OVER w_ordered AS running_max_status_code_high_rank,
        SUM(status_code_high_rank) OVER w_ordered AS running_sum_status_code_high_rank,
        AVG(status_code_high_rank) OVER w_ordered AS running_avg_status_code_high_rank,
        MIN(status_code_high_rank) OVER w_ordered AS running_min_status_code_high_rank,
        NTILE(10) OVER (PARTITION BY k, level ORDER BY flag_bit8_avg_5 ASC, id ASC, k ASC) AS flag_bit8_avg_5_bucket
    FROM scored
    WINDOW w_ordered AS (PARTITION BY k ORDER BY flag_bit8 ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY running_max_status_code_high_rank DESC NULLS LAST, id ASC, k ASC) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY k ORDER BY running_sum_status_code_high_rank DESC) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        running_max_status_code_high_rank - running_sum_status_code_high_rank AS running_max_status_code_high_rank_minus_running_sum_status_code_high_rank,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM ranked
    WHERE flag_bit8 BETWEEN 0 AND 1
)
SELECT
    id,
    k,
    attr_value,
    flag_bit8,
    running_max_status_code_high_rank,
    running_sum_status_code_high_rank,
    running_min_status_code_high_rank,
    row_num,
    running_max_status_code_high_rank_minus_running_sum_status_code_high_rank,
    CASE WHEN k RLIKE '^(trace|region)$' THEN 'GROUP_B' WHEN k RLIKE '^(version)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS k_zone
FROM filtered
ORDER BY k_zone DESC NULLS LAST;
