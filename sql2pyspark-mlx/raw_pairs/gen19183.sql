WITH flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    browser,
    country,
    MAX(started_at) AS ended_at,
    APPROX_PERCENTILE(session_id, 0.95) AS session_id_p95,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS session_id_per_uk
FROM islands
GROUP BY GROUPING SETS ((browser, country), (browser))
ORDER BY session_id_p95 DESC NULLS LAST
