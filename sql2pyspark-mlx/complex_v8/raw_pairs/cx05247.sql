WITH base AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE event_type = 'created'
      AND actor = 'system'
),
latest AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id DESC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
qualified_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority
    FROM qualified_2 s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS rn,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_priority ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_priority ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM combined_details_ticket
),
rollup_base AS (
    -- Group-level totals
    SELECT
        actor,
        ROUND(STDDEV(rn), 2) AS stddev_rn,
        MAX(rn) AS max_rn,
        COUNT(*) AS cnt,
        SUM(rn) FILTER (WHERE actor IN ('customer', 'agent')) AS rn_actor_in_customer_agent
    FROM with_trends
    GROUP BY 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT actor, metric_name, reading
FROM (
    SELECT actor, cnt, max_rn
    FROM rollup_base
) src
UNPIVOT (
    reading FOR metric_name IN (cnt AS m_cnt, max_rn AS m_max_rn)
) unp
ORDER BY actor, metric_name
