WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 7 AND 11
      AND ticket_id <= 56
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM scoped s
),
latest AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_or_unknown,
        first_event_at,
        event_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY first_event_at DESC, id DESC) AS rn
        FROM with_counts
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(event_count) OVER (PARTITION BY event_count ORDER BY event_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_count,
        SUM(event_count) OVER (PARTITION BY event_count ORDER BY event_count, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_count_sum_2_prior,
        COUNT(*) OVER (PARTITION BY event_count, actor) AS rows_in_event_count_actor
    FROM latest
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_id ORDER BY first_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY first_event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(running_event_count / NULLIF(SUM(running_event_count) OVER (PARTITION BY event_count), 0), 4) AS running_event_count_share_of_event_count
    FROM with_trends
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.closed_at AS ticket_closed_at, su.hired_on AS agent_hired_on
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, minutes_since_prev_event_at, running_event_count_share_of_event_count
    FROM with_parent_ticket
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (minutes_since_prev_event_at, running_event_count_share_of_event_count)
) unp
ORDER BY id, measure
