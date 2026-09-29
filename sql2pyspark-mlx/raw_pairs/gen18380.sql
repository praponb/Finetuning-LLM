WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
)
-- Window metrics over several different partitions
SELECT
    host,
    status_code,
    k,
    value,
    MAX(status_code) OVER w_grp AS service_max_status_code,
    CUME_DIST() OVER w_part AS status_code_cume_dist,
    PERCENT_RANK() OVER w_part AS status_code_percent_rank,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY level, k), 4) AS status_code_share_2
FROM lines
WINDOW w_grp AS (PARTITION BY service), w_part AS (ORDER BY status_code DESC)
