WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-08-14 09:00:00'
      AND country IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    browser,
    MAX(started_at) AS last_started_at,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    PERCENTILE_APPROX(session_no, 0.95) AS session_no_p95,
    SUM(CASE WHEN country = 'UK' THEN session_start_flag ELSE 0 END) AS uk_session_start_flag
FROM islands
GROUP BY browser
HAVING COUNT(CASE WHEN country = 'TH' THEN 1 END) > 1
ORDER BY th_count DESC NULLS FIRST;
