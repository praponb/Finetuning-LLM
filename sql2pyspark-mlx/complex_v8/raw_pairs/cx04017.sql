WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY event_at ASC, id), 3) AS event_at_cume_dist
    FROM covered
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        second_event_at,
        ROUND(event_at_cume_dist - AVG(event_at_cume_dist) OVER w_event_at_year_ticket_id, 2) AS event_at_cume_dist_vs_event_at_year_ticket_id_av,
        LEAD(event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS next3_event_at,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER (PARTITION BY YEAR(event_at), ticket_id), 0), 4) AS event_at_cume_dist_share_of_event_at_year_ticket,
        LAG(second_event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS prev3_second_event_at,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_rank,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_quintile
    FROM sequenced
    WINDOW w_event_at_year_ticket_id AS (PARTITION BY YEAR(event_at), ticket_id)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, ROUND(AVG(requester_id), 2) AS avg_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.avg_ticket_requester_id
    FROM with_trends s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    event_at,
    priority,
    agent_id,
    hours_since_open,
    second_event_at,
    event_at_cume_dist_vs_event_at_year_ticket_id_av,
    next3_event_at,
    prev3_second_event_at,
    event_at_quintile,
    avg_ticket_requester_id,
    CASE WHEN event_type IN ('resolved', 'assigned') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM summarized_rows
ORDER BY prev3_second_event_at
