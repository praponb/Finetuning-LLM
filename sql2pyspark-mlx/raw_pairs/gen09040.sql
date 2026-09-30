WITH lines AS (
    SELECT
        id,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
unpacked AS (
    SELECT
        id,
        status_code,
        k,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
cleaned AS (
    SELECT
        id,
        k,
        status_code + status_code_high AS status_code_plus_status_code_high
    FROM unpacked
    WHERE k IS DISTINCT FROM 'version'
),
latest AS (
    SELECT
        id,
        k,
        status_code_plus_status_code_high
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_plus_status_code_high DESC NULLS FIRST, id, k) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    SELECT
        id,
        k,
        status_code_plus_status_code_high,
        SUM(status_code_plus_status_code_high) OVER (PARTITION BY k) AS k_sum_status_code_plus_status_code_high,
        LAG(status_code_plus_status_code_high, 1) OVER w_recent AS prev_status_code_plus_status_code_high,
        LEAD(status_code_plus_status_code_high, 2) OVER w_recent AS next_status_code_plus_status_code_high,
        LAST_VALUE(status_code_plus_status_code_high) IGNORE NULLS OVER (PARTITION BY k ORDER BY status_code_plus_status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_code_plus_status_code_high,
        LEAD(status_code_plus_status_code_high, 1, 0) OVER (PARTITION BY k ORDER BY status_code_plus_status_code_high, id, k) AS next_status_code_plus_status_code_high_2
    FROM latest
    WHERE k IN ('trace', 'region', 'version')
    WINDOW w_recent AS (ORDER BY status_code_plus_status_code_high, id, k)
),
metrics_app_logs AS (
    SELECT
        id,
        k,
        next_status_code_plus_status_code_high,
        last_known_status_code_plus_status_code_high,
        next_status_code_plus_status_code_high_2,
        MAX(last_known_status_code_plus_status_code_high) OVER w_running AS k_max_last_known_status_code_plus_status_code_high,
        AVG(k_sum_status_code_plus_status_code_high) OVER (PARTITION BY k ORDER BY next_status_code_plus_status_code_high_2, id, k ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS k_sum_status_code_plus_status_code_high_avg_5_prior,
        STDDEV_SAMP(next_status_code_plus_status_code_high_2) OVER w_running AS k_stddev_samp_next_status_code_plus_status_code_high_2
    FROM metrics
    WINDOW w_running AS (PARTITION BY k)
)
SELECT
    id,
    k,
    next_status_code_plus_status_code_high,
    k_max_last_known_status_code_plus_status_code_high,
    k_sum_status_code_plus_status_code_high_avg_5_prior,
    k_stddev_samp_next_status_code_plus_status_code_high_2,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY k_stddev_samp_next_status_code_plus_status_code_high_2 DESC, id, k) AS event_seq,
    AVG(next_status_code_plus_status_code_high) OVER w_part AS next_status_code_plus_status_code_high_avg_6_prior
FROM metrics_app_logs
WINDOW w_part AS (PARTITION BY k ORDER BY next_status_code_plus_status_code_high_2, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
