WITH exploded AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
)
SELECT
    key,
    host,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    AVG(latency_ms) FILTER (WHERE key = 'region') AS region_avg_latency_ms
FROM exploded
GROUP BY 1, 2
ORDER BY latency_ms_bit_or DESC NULLS FIRST;
