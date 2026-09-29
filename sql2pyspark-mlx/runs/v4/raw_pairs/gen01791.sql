WITH lines AS (
    SELECT
        id,
        host,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(host)) AS host_title,
        COALESCE(key, 'NONE') AS key_filled
    FROM lines
    WHERE key IS NOT NULL
),
valid AS (
    SELECT
        *,
        TRIM(key) AS clean_key,
        CASE WHEN host = 'h4' THEN 1 ELSE 0 END AS is_h4
    FROM cleaned
    WHERE host = 'h2'
),
matched_keys AS (
    SELECT id, host
    FROM valid
    INTERSECT
    SELECT id, host
    FROM app_logs
    WHERE latency_ms <> 922
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.host = k.host
),
stats AS (
    SELECT
        CASE WHEN is_h4 >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_h4_tier,
        AVG(is_h4) AS avg_is_h4,
        COUNT(CASE WHEN host = 'h1' THEN 1 END) AS h1_count,
        COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
        ROUND(AVG(is_h4), 2) AS avg_is_h4_2
    FROM kept_rows
    GROUP BY CASE WHEN is_h4 >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING avg_is_h4 >= 0
)
SELECT is_h4_tier, h1_count
FROM stats
ORDER BY is_h4_tier DESC NULLS FIRST, h1_count
LIMIT 5;
