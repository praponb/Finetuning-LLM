WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY log_ts DESC, id, attr_key) AS rn
        FROM flattened
    ) t
    WHERE rn = 1
),
metrics AS (
    SELECT
        *,
        LEAD(latency_ms, 1) OVER (PARTITION BY attr_key ORDER BY rn, id, attr_key) AS next_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, attr_key), 4) AS latency_ms_share,
        NTILE(4) OVER (ORDER BY status_code, id, attr_key) AS status_code_bucket,
        SUM(status_code) OVER (ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        AVG(status_code) OVER (PARTITION BY host ORDER BY status_code, id, attr_key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_code_avg_3_prior
    FROM deduped
    WHERE attr_key = 'trace'
),
cleaned AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        value,
        rn,
        next_latency_ms,
        latency_ms_share,
        cumulative_status_code,
        status_code_avg_3_prior,
        ROUND(status_code_bucket * 100, 2) AS status_code_bucket_scaled,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM metrics
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'region'
)
SELECT
    id,
    attr_key,
    metric,
    reading
FROM (
    SELECT
        id,
        attr_key,
        latency_ms_share,
        status_code_bucket_scaled
    FROM cleaned
) src
UNPIVOT (
    reading FOR metric IN (latency_ms_share, status_code_bucket_scaled AS STATUS_CODE_BUCKET_SCALED)
);
