WITH base AS (
    SELECT
        id,
        page,
        event_time,
        CONCAT_WS('/', event_type, page) AS event_type_page_key
    FROM web_events
    WHERE page IS NOT NULL
      AND page IS NOT DISTINCT FROM '/home'
),
valid AS (
    SELECT
        *,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(page, 'NONE') AS page_clean
    FROM base
    WHERE page IS NOT NULL
),
prev_events AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM valid
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_web_events AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time_day DESC NULLS LAST, id) AS rank_in_group
        FROM sessions_web_events
    ) ranked
    WHERE rank_in_group = 1
),
base_web_events AS (
    SELECT *, island_id + rank_in_group AS island_id_plus_rank_in_group
    FROM deduped
    WHERE page IS NOT DISTINCT FROM '/cart'
      AND event_time > TO_TIMESTAMP('2023-06-19 15:00:00')
),
valid_web_events AS (
    SELECT
        id,
        event_time,
        event_time_day,
        prev_event_time,
        session_start_flag,
        island_id,
        rank_in_group,
        island_id_plus_rank_in_group,
        COALESCE(session_start_flag, 0) AS session_start_flag_filled,
        session_start_flag + island_id AS session_start_flag_plus_island_id,
        COALESCE(page, 'N/A') AS page_clean_2
    FROM base_web_events
    WHERE event_time > DATE '2023-03-10'
      AND island_id_plus_rank_in_group <= 71
)
SELECT
    id,
    event_time,
    event_time_day,
    prev_event_time,
    session_start_flag,
    island_id,
    rank_in_group,
    island_id_plus_rank_in_group,
    session_start_flag_filled,
    session_start_flag_plus_island_id,
    page_clean_2,
    LAG(session_start_flag_plus_island_id) OVER w_grp AS prev_session_start_flag_plus_island_id,
    LEAD(session_start_flag_plus_island_id) OVER w_grp AS next_session_start_flag_plus_island_id,
    LAG(island_id) OVER w_grp AS prev_island_id,
    LEAD(island_id) OVER w_grp AS next_island_id,
    ROW_NUMBER() OVER w_grp AS event_seq
FROM valid_web_events
WINDOW w_grp AS (ORDER BY event_time, id);
