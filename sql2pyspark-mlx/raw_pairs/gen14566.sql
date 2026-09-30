WITH valid AS (
    SELECT *, ROUND(duration_sec, 1) AS duration_sec_rounded
    FROM web_events
    WHERE page IS NOT NULL
      AND duration_sec < 456
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY page ORDER BY is_new_session DESC, id) AS is_new_session_bucket,
        AVG(duration_sec_rounded) OVER (PARTITION BY session_id, page ORDER BY is_new_session, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_rounded_avg_2_prior,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec_rounded DESC) AS duration_sec_rounded_cume_dist,
        SUM(duration_sec_rounded) OVER (PARTITION BY session_id ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_rounded
    FROM sessionized
),
enriched_web_events AS (
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.duration_sec,
        a.burst_id,
        a.duration_sec_rounded_avg_2_prior,
        b.browser
    FROM enriched a
    LEFT JOIN sessions b
        ON a.session_id = b.id
    WHERE b.started_at < '2025-04-01'
),
scored AS (
    SELECT
        *,
        SUM(burst_id) OVER (PARTITION BY session_id, browser ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_burst_id,
        COUNT(burst_id) OVER (PARTITION BY session_id, browser ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_burst_id,
        MAX(burst_id) OVER (PARTITION BY session_id, browser ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_burst_id,
        LEAD(duration_sec_rounded_avg_2_prior, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) AS next_duration_sec_rounded_avg_2_prior
    FROM enriched_web_events
)
SELECT
    d.country,
    a.browser AS web_event_browser,
    COUNT(*) AS rows_total,
    MIN(a.last3_sum_burst_id) AS min_last3_sum_burst_id,
    GROUPING_ID() AS grouping_level
FROM scored a
LEFT JOIN sessions d ON a.session_id = d.id
GROUP BY GROUPING SETS ((d.country, a.browser), (d.country), ());
