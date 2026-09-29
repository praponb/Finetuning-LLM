WITH exploded AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
enriched AS (
    SELECT
        *,
        MAX(status_code) OVER w_part AS last3_max_status_code,
        SUM(status_code) OVER w_part AS last3_sum_status_code,
        MIN(status_code) OVER w_part AS last3_min_status_code,
        COUNT(status_code) OVER w_part AS last3_count_status_code,
        CORR(status_code, latency_ms) OVER (PARTITION BY attr_key) AS corr_status_code_latency_ms
    FROM exploded
    WHERE latency_ms IS NOT NULL
    WINDOW w_part AS (PARTITION BY host, attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level, attr_key ORDER BY last3_count_status_code DESC) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY rank_in_group, id, attr_key) AS row_seq,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        LAST_VALUE(attr_key, TRUE) OVER (PARTITION BY level ORDER BY last3_count_status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
        AVG(latency_ms) OVER (PARTITION BY host, attr_key ORDER BY corr_status_code_latency_ms, id, attr_key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_3_prior
    FROM latest
),
enriched_app_logs AS (
    SELECT
        *,
        COUNT(status_code_share) OVER (PARTITION BY host) AS host_count_status_code_share,
        LAG(corr_status_code_latency_ms, 2) OVER (PARTITION BY attr_key ORDER BY last3_sum_status_code, id, attr_key) AS prev_corr_status_code_latency_ms
    FROM with_windows
)
SELECT
    id,
    status_code,
    attr_key,
    last3_max_status_code,
    last3_min_status_code,
    last3_count_status_code,
    corr_status_code_latency_ms,
    last_known_attr_key,
    latency_ms_avg_3_prior,
    host_count_status_code_share,
    prev_corr_status_code_latency_ms,
    ROW_NUMBER() OVER (ORDER BY corr_status_code_latency_ms DESC, id, attr_key) AS row_seq_2,
    COUNT(host_count_status_code_share) OVER (PARTITION BY host, attr_key) AS host_count_host_count_status_code_share,
    ROUND(last3_sum_status_code / SUM(last3_sum_status_code) OVER (PARTITION BY host), 4) AS last3_sum_status_code_share
FROM enriched_app_logs
