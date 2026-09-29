WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY level, service ORDER BY status_code ASC) AS status_code_rank,
        AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m,
        CUME_DIST() OVER (PARTITION BY level, attr_key ORDER BY status_code ASC) AS status_code_cume_dist,
        MAX(status_code) OVER (PARTITION BY level) AS level_max_status_code
    FROM exploded
),
with_windows AS (
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY level, attr_key) AS level_avg_status_code,
        NTILE(10) OVER (PARTITION BY YEAR(log_ts), level ORDER BY level_max_status_code ASC, id ASC, attr_key ASC) AS level_max_status_code_bucket
    FROM enriched
),
flagged AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts ASC, id ASC, attr_key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts ASC, id ASC, attr_key ASC) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY service ORDER BY log_ts ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
wide AS (
    SELECT *
    FROM (
        SELECT
            service,
            attr_key,
            level_max_status_code_bucket
        FROM sessionized
    ) src
    PIVOT (
        MAX(level_max_status_code_bucket) AS value_sum,
        COUNT(1) AS n
        FOR attr_key IN ('region', 'version')
    )
)
SELECT
    service,
    region_value_sum,
    region_n,
    version_n,
    CASE WHEN service RLIKE '^(billing|checkout)$' THEN 'PRIORITY' ELSE 'CORE' END AS service_zone
FROM wide
ORDER BY service_zone ASC NULLS LAST;
