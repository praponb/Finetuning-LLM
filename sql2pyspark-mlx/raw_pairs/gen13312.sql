WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(latency_ms, status_code) OVER (PARTITION BY attr_key, service) AS corr_latency_ms_status_code,
        MAX(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_max_latency_ms,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(log_ts), service ORDER BY status_code DESC) AS status_code_dense_rank,
        RANK() OVER w_running AS status_code_rank
    FROM lines
    WHERE level IN ('WARN', 'DEBUG')
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY status_code DESC)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, attr_key_max_latency_ms * latency_ms AS attr_key_max_latency_ms_times_latency_ms
    FROM scored
    WHERE log_ts >= DATE '2025-06-01'
      AND attr_key IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY corr_latency_ms_status_code NULLS FIRST) AS corr_latency_ms_status_code_cume_dist,
        RANK() OVER w_part AS status_code_dense_rank_rank,
        ROUND(status_code_rank / SUM(status_code_rank) OVER (PARTITION BY service), 4) AS status_code_rank_share,
        ROUND(attr_key_max_latency_ms / SUM(attr_key_max_latency_ms) OVER (PARTITION BY level, service), 4) AS attr_key_max_latency_ms_share,
        PERCENT_RANK() OVER w_running AS status_code_rank_percent_rank
    FROM base
    WINDOW w_part AS (PARTITION BY level ORDER BY status_code_dense_rank NULLS FIRST), w_running AS (PARTITION BY YEAR(log_ts), attr_key ORDER BY status_code_rank DESC NULLS LAST)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', attr_key, level) AS attr_key_level_key,
        COALESCE(attr_key, 'OTHER') AS attr_key_clean
    FROM enriched
    WHERE attr_key_max_latency_ms_times_latency_ms BETWEEN 24 AND 56
      AND attr_key IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        service,
        latency_ms,
        attr_key,
        attr_value,
        attr_key_max_latency_ms,
        status_code_dense_rank,
        status_code_rank,
        corr_latency_ms_status_code_cume_dist,
        status_code_rank_share,
        status_code_rank_percent_rank,
        attr_key_level_key,
        attr_key_clean,
        LENGTH(service) AS service_length
    FROM filtered
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'version'
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        attr_value,
        attr_key_max_latency_ms,
        status_code_rank,
        corr_latency_ms_status_code_cume_dist,
        status_code_rank_share,
        status_code_rank_percent_rank,
        attr_key_level_key,
        attr_key_clean,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code_rank_percent_rank DESC NULLS LAST) AS status_code_rank_percent_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_dense_rank) AS status_code_dense_rank_percent_rank
    FROM cleaned
),
report AS (
    SELECT
        attr_value,
        attr_key_max_latency_ms,
        status_code_rank,
        status_code_rank_percent_rank,
        attr_key_clean,
        status_code_rank_percent_rank_percent_rank,
        status_code_dense_rank_percent_rank
    FROM enriched_app_logs
)
SELECT attr_key_max_latency_ms, status_code_dense_rank_percent_rank
FROM report
ORDER BY attr_key_max_latency_ms DESC, status_code_dense_rank_percent_rank;
