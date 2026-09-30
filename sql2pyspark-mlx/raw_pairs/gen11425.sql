WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        CASE WHEN page = '/search' THEN 1 ELSE 0 END AS is_search,
        TRIM(event_type) AS clean_event_type
    FROM web_events
    WHERE TO_DATE(event_time) < '2023-03-01'
)
-- Ranking and gap features
SELECT
    id,
    event_time,
    is_search,
    AVG(is_search) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_search_avg_5_prior,
    PERCENT_RANK() OVER (ORDER BY is_search DESC NULLS LAST) AS is_search_percent_rank,
    NTILE(10) OVER (PARTITION BY page, session_id ORDER BY is_search DESC, id) AS is_search_bucket,
    PERCENT_RANK() OVER w_recent AS is_search_percent_rank_2,
    AVG(duration_sec) OVER (PARTITION BY session_id, page ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior
FROM filtered
WINDOW w_recent AS (ORDER BY is_search DESC);
