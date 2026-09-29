WITH lines AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
parsed AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE level = 'ERROR'
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY flag_bit8) AS flag_bit8_percent_rank,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC) AS status_code_high_dense_rank,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank
    FROM parsed
    WHERE host NOT IN ('h4', 'h3', 'h2')
),
filtered AS (
    SELECT
        *,
        status_code_high_dense_rank * flag_bit8_percent_rank AS status_code_high_dense_rank_times_flag_bit8_percent_rank,
        ROUND(latency_ms_dense_rank, 2) AS latency_ms_dense_rank_rounded
    FROM with_windows
    WHERE flag_bit8 > 1
),
cleaned AS (
    SELECT
        *,
        SUBSTRING(host, 1, 2) AS host_prefix,
        COALESCE(flag_bit8, 0) AS flag_bit8_filled
    FROM filtered
    WHERE status_code_high_dense_rank IS NOT NULL
),
summary AS (
    SELECT
        CASE WHEN flag_bit8 >= 1 THEN 'HIGH' ELSE 'LOW' END AS flag_bit8_tier,
        host,
        BIT_AND(latency_ms) AS latency_ms_bit_and,
        SORT_ARRAY(COLLECT_SET(service)) AS service_list,
        COUNT_IF(host = 'h4') AS h4_events,
        SUM(CASE WHEN level = 'INFO' THEN flag_bit8_percent_rank ELSE 0 END) AS info_flag_bit8_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN flag_bit8 >= 1 THEN 'HIGH' ELSE 'LOW' END ORDER BY BIT_AND(latency_ms) DESC) AS latency_ms_bit_and_percent_rank
    FROM cleaned
    GROUP BY CASE WHEN flag_bit8 >= 1 THEN 'HIGH' ELSE 'LOW' END, host
    HAVING COUNT_IF(host = 'h4') < 1000
)
SELECT
    flag_bit8_tier,
    host,
    h4_events,
    info_flag_bit8_percent_rank,
    latency_ms_bit_and_percent_rank
FROM summary
ORDER BY info_flag_bit8_percent_rank;
