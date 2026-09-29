WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_2
    FROM web_events
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY cumulative_duration_sec_2 ASC NULLS LAST, id ASC) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        cumulative_duration_sec
    FROM (
        SELECT *, RANK() OVER (PARTITION BY page ORDER BY cumulative_duration_sec ASC NULLS LAST) AS rank_in_group
        FROM deduped
    ) t
    WHERE rank_in_group = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT src.*, s.browser
    FROM ranked src
    INNER JOIN sessions s
        ON src.session_id = s.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        cumulative_duration_sec,
        CUME_DIST() OVER (PARTITION BY browser ORDER BY cumulative_duration_sec DESC NULLS LAST) AS cumulative_duration_sec_cume_dist,
        MIN(cumulative_duration_sec) OVER (PARTITION BY browser, session_id) AS browser_min_cumulative_duration_sec
    FROM joined
)
-- Grouped metrics
SELECT
    session_id,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    ROUND(AVG(cumulative_duration_sec_cume_dist), 2) AS avg_cumulative_duration_sec_cume_dist,
    APPROX_PERCENTILE(cumulative_duration_sec, 0.5) AS cumulative_duration_sec_p50
FROM with_windows
GROUP BY session_id
ORDER BY home_count DESC NULLS FIRST
