WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_type = 'escalated'
      AND actor IN ('system', 'agent')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        LAST_VALUE(event_at_date) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_date,
        DATEDIFF(event_at_date, LAG(event_at_date) OVER w_all_by_event_at) AS days_since_prev_event_at_date,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS rn
    FROM base
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
actor_stats AS (
    -- Max days_since_prev_event_at_date per actor
    SELECT actor, MAX(days_since_prev_event_at_date) AS max_days_since_prev_event_at_date
    FROM with_windows
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_prev_event_at_date,
        ROUND(s.days_since_prev_event_at_date - g.max_days_since_prev_event_at_date, 2) AS days_since_prev_event_at_date_minus_max
    FROM with_windows s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_prev_event_at_date = g.max_days_since_prev_event_at_date
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.days_since_prev_event_at_date,
        s.rn,
        s.max_days_since_prev_event_at_date,
        s.days_since_prev_event_at_date_minus_max,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        su.agent_name
    FROM outliers s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM combined_details s
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.days_since_prev_event_at_date,
        s.rn,
        s.max_days_since_prev_event_at_date,
        s.ticket_priority,
        s.ticket_tags,
        s.agent_name,
        s.last_event_at,
        s.last_event_at_2,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at AS event_event_at
    FROM profiled s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'system'
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        days_since_prev_event_at_date,
        rn,
        max_days_since_prev_event_at_date,
        ticket_priority,
        agent_name,
        last_event_at,
        last_event_at_2,
        event_id,
        event_actor,
        event_type,
        event_event_at,
        NTILE(10) OVER (PARTITION BY YEAR(last_event_at) ORDER BY max_days_since_prev_event_at_date NULLS LAST, id, event_id) AS max_days_since_prev_event_at_date_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(last_event_at) ORDER BY max_days_since_prev_event_at_date NULLS LAST), 3) AS max_days_since_prev_event_at_date_percent_rank
    FROM detail_rows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, measure, reading
FROM (
    SELECT id, event_id, days_since_prev_event_at_date, max_days_since_prev_event_at_date_decile, rn
    FROM with_windows_2
) src
UNPIVOT (
    reading FOR measure IN (days_since_prev_event_at_date, max_days_since_prev_event_at_date_decile, rn)
) unp
