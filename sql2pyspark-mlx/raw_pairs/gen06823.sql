WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
parsed AS (
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE service = 'auth'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY service ORDER BY has_fault DESC) AS rn
        FROM parsed
    ) ranked
    WHERE rn = 1
),
latest_app_logs AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code DESC, id ASC, attr_key ASC) AS row_num
    FROM latest
    QUALIFY row_num = 1
),
valid AS (
    SELECT
        *,
        CASE WHEN service = 'billing' THEN 1 ELSE 0 END is_billing,
        COALESCE(attr_key, 'OTHER') resolved_attr_key
    FROM latest_app_logs
    WHERE status_code_high > 1
),
stats AS (
    SELECT
        CASE WHEN status_code >= 443 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
        BIT_AND(status_code) AS status_code_bit_and,
        COUNT(DISTINCT attr_key) AS distinct_attr_keys,
        COUNT_IF(service = 'auth') AS auth_events,
        MIN(status_code_high) AS min_status_code_high,
        ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS status_code_per_region,
        SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list
    FROM valid
    GROUP BY CASE WHEN status_code >= 443 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    status_code_tier,
    auth_events,
    min_status_code_high,
    status_code_per_region,
    attr_key_list
FROM stats
ORDER BY auth_events ASC NULLS LAST;
