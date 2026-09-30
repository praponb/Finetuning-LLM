WITH lines AS (
    SELECT
        id,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
filtered AS (
    SELECT
        id,
        level,
        latency_ms,
        k,
        CONCAT(k, ' ', level) AS k_level,
        COALESCE(k, 'N/A') AS resolved_k
    FROM lines
    WHERE k IS NOT NULL
),
filtered_app_logs AS (
    SELECT
        id,
        k,
        resolved_k,
        CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END AS is_error
    FROM filtered
    WHERE latency_ms BETWEEN 1011 AND 2254
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY is_error DESC NULLS FIRST, id, k) AS rn
    FROM filtered_app_logs
    QUALIFY rn = 1
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY k ORDER BY is_error DESC NULLS FIRST) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
latest_app_logs AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY is_error DESC, id, k) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
grouped AS (
    SELECT
        k,
        CASE WHEN is_error >= 73 THEN 'LARGE' ELSE 'SMALL' END AS is_error_tier,
        SIZE(COLLECT_SET(k)) AS num_ks,
        COUNT_IF(k = 'region') AS region_events,
        SIZE(COLLECT_SET(k)) AS num_ks_2
    FROM latest_app_logs
    GROUP BY GROUPING SETS ((k, CASE WHEN is_error >= 73 THEN 'LARGE' ELSE 'SMALL' END), (k), ())
    HAVING COUNT_IF(k = 'region') >= 1
)
SELECT
    k,
    is_error_tier,
    num_ks
FROM grouped
