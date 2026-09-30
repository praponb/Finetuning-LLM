WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        service,
        status_code,
        attr_key,
        LENGTH(attr_key) attr_key_length,
        TRIM(service) clean_service,
        COALESCE(attr_key, 'NONE') attr_key_clean
    FROM lines
    WHERE latency_ms BETWEEN 1096 AND 1514
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(status_code, 2) status_code_rounded,
        COALESCE(attr_key, 'DIRECT') attr_key_filled
    FROM filtered
    WHERE attr_key_length <= 38
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        service,
        status_code,
        attr_key,
        attr_key_length,
        clean_service,
        attr_key_filled,
        CASE WHEN service = 'billing' THEN 1 ELSE 0 END is_billing
    FROM base
    WHERE status_code_rounded BETWEEN 54 AND 71
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code >= 405 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    CASE WHEN is_billing >= 63 THEN 'LARGE' ELSE 'SMALL' END AS is_billing_tier,
    PERCENTILE_APPROX(attr_key_length, ARRAY(0.25, 0.5, 0.75)) AS attr_key_length_quartiles,
    COUNT(*) AS event_count,
    COUNT(DISTINCT service) AS distinct_services
FROM cleaned
GROUP BY status_code_tier, is_billing_tier
ORDER BY event_count
