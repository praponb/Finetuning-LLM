WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS LAST, id) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY event_time DESC, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY session_id_2 DESC, id) AS dedup_rank
        FROM islands
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    event_time,
    session_id_2,
    dedup_rank
FROM latest;
