WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 07:00:00'
      AND agent_id IS NOT NULL
),
joined_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND su.agent_name IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(requester_id_filled) OVER w_closed_at_to_date AS closed_at_to_date_min_requester_id_filled,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY TO_DATE(closed_at))) AS distinct_agent_id_in_closed_at_to_date,
        RANK() OVER w_all_by_closed_at AS closed_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY opened_at), 3) AS opened_at_cume_dist,
        RANK() OVER (ORDER BY closed_at DESC) AS closed_at_rank_2
    FROM joined_agent
    WINDOW w_closed_at_to_date AS (PARTITION BY TO_DATE(closed_at)), w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.closed_at_hour,
        s.requester_id_filled,
        s.agent_name,
        s.closed_at_to_date_min_requester_id_filled,
        s.distinct_agent_id_in_closed_at_to_date,
        s.closed_at_rank,
        s.opened_at_cume_dist,
        s.closed_at_rank_2,
        t.tag_pos,
        t.tag
    FROM sequenced s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.closed_at_hour,
        s.requester_id_filled,
        s.distinct_agent_id_in_closed_at_to_date,
        s.closed_at_rank,
        s.opened_at_cume_dist,
        s.closed_at_rank_2,
        s.tag_pos,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM exploded s
    JOIN ticket_events t ON t.ticket_id = s.id AND TO_DATE(t.event_at) < DATE '2025-01-31'
)
-- Subtotals with GROUPING SETS
SELECT
    csat,
    distinct_agent_id_in_closed_at_to_date,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN priority NOT IN ('P1', 'P3') THEN closed_at_rank_2 ELSE 0 END) AS closed_at_rank_2_priority_not_in_p1_p3,
    COUNT(*) AS cnt,
    COUNT(DISTINCT channel) AS distinct_channels
FROM exploded_detail_events
GROUP BY GROUPING SETS ((csat, distinct_agent_id_in_closed_at_to_date), (csat), (distinct_agent_id_in_closed_at_to_date), ())
ORDER BY csat NULLS LAST, distinct_agent_id_in_closed_at_to_date NULLS LAST
