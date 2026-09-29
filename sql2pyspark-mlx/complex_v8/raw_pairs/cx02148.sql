WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(is_new_session / NULLIF(SUM(is_new_session) OVER (PARTITION BY session_id, event_type), 0), 4) AS is_new_session_share_of_session_id_event_type,
        MIN(is_new_session) OVER (PARTITION BY session_id, event_type) AS session_id_event_type_min_is_new_session,
        SUM(session_id) OVER (PARTITION BY TO_DATE(prev_event_at) ORDER BY is_new_session ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS session_id_sum_3,
        NTILE(5) OVER (PARTITION BY TO_DATE(prev_event_at) ORDER BY is_new_session ASC, id) AS is_new_session_quintile,
        FIRST_VALUE(session_id) IGNORE NULLS OVER (PARTITION BY TO_DATE(prev_event_at) ORDER BY is_new_session ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_id
    FROM sessionized
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN first_known_session_id >= 5 THEN 'A' WHEN first_known_session_id >= 3 THEN 'B' ELSE 'C' END AS first_known_session_id_tier
    FROM with_windows
    WHERE event_type IN ('reopened', 'replied', 'assigned')
)
SELECT id, field, value
FROM (
    SELECT id, is_new_session_quintile, first_known_session_id
    FROM scored
) src
UNPIVOT (
    value FOR field IN (is_new_session_quintile, first_known_session_id)
) unp
