WITH joined AS (
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.duration_sec,
        p.browser,
        p.country
    FROM web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
    WHERE p.browser = 'Edge'
),
with_windows AS (
    SELECT
        id,
        session_id,
        event_time,
        country,
        CUME_DIST() OVER (PARTITION BY browser ORDER BY duration_sec) AS duration_sec_cume_dist,
        SUM(duration_sec) OVER (PARTITION BY country ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM joined
),
enriched AS (
    SELECT
        src.id,
        src.session_id,
        src.event_time,
        src.country,
        src.duration_sec_cume_dist,
        src.running_duration_sec,
        s.country AS session_country
    FROM with_windows src
    LEFT JOIN sessions s
        ON src.session_id = s.id
),
cleaned AS (
    SELECT
        id,
        session_id,
        event_time,
        country,
        duration_sec_cume_dist,
        COALESCE(duration_sec_cume_dist, 0) AS duration_sec_cume_dist_filled,
        COALESCE(session_country, 'OTHER') AS resolved_session_country
    FROM enriched
    WHERE country NOT IN ('SG', 'DE')
      AND country IS NOT NULL
),
with_windows_web_events AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec_cume_dist,
        duration_sec_cume_dist_filled,
        SUM(duration_sec_cume_dist) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_duration_sec_cume_dist,
        COUNT(duration_sec_cume_dist) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_duration_sec_cume_dist,
        MIN(duration_sec_cume_dist) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_duration_sec_cume_dist
    FROM cleaned
)
SELECT
    session_id,
    CASE WHEN last3_min_duration_sec_cume_dist >= 0.72 THEN 'LARGE' ELSE 'SMALL' END AS last3_min_duration_sec_cume_dist_tier,
    MIN(event_time) AS first_seen,
    APPROX_PERCENTILE(last3_sum_duration_sec_cume_dist, 0.9) AS last3_sum_duration_sec_cume_dist_p90,
    COUNT(DISTINCT session_id) AS distinct_session_ids,
    MAX(last3_min_duration_sec_cume_dist) AS max_last3_min_duration_sec_cume_dist
FROM with_windows_web_events
GROUP BY session_id, last3_min_duration_sec_cume_dist_tier
