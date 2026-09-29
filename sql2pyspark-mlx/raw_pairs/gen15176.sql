WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
parsed AS (
    SELECT
        id,
        host,
        attr_key,
        attr_value,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
with_windows AS (
    SELECT
        id,
        host,
        attr_key,
        has_fault,
        NTILE(10) OVER (ORDER BY status_code_high ASC, id ASC, attr_key ASC) AS status_code_high_bucket,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code_high DESC) AS status_code_high_cume_dist,
        ROW_NUMBER() OVER (ORDER BY status_code_high ASC, id ASC, attr_key ASC) AS row_seq
    FROM parsed
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY status_code_high_bucket ASC NULLS FIRST, id ASC, attr_key ASC) AS rn
    FROM with_windows
    QUALIFY rn = 1
)
SELECT
    CASE WHEN status_code_high_cume_dist >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_cume_dist_tier,
    attr_key,
    CASE WHEN status_code_high_bucket >= 6 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_bucket_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(status_code_high_cume_dist), 2) AS avg_status_code_high_cume_dist,
    ROUND(SUM(has_fault) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS has_fault_per_region,
    COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    SUM(CASE WHEN host = 'h4' THEN status_code_high_bucket ELSE 0 END) AS h4_status_code_high_bucket
FROM deduped
GROUP BY status_code_high_cume_dist_tier, attr_key, status_code_high_bucket_tier
ORDER BY h4_status_code_high_bucket DESC;
