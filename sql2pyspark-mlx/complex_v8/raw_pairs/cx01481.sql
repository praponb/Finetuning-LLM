WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND HOUR(event_at) BETWEEN 10 AND 13
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2,
        SUM(requester_id) AS total_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2,
        t.total_ticket_requester_id
    FROM with_parent s
    LEFT JOIN ticket_stats t ON t.agent_id = s.ticket_agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        actor,
        ticket_opened_at,
        ticket_agent_id,
        ticket_count,
        ticket_count_2,
        LAG(ticket_count) OVER w_ticket_opened_at_to_date_act AS prev_ticket_count,
        RANK() OVER (PARTITION BY ticket_count ORDER BY ticket_count_2 DESC, id) AS ticket_count_2_rank,
        NTH_VALUE(total_ticket_requester_id, 2) OVER (ORDER BY ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_ticket_requester_id
    FROM summarized_rows
    WINDOW w_ticket_opened_at_to_date_act AS (PARTITION BY TO_DATE(ticket_opened_at), actor ORDER BY ticket_count DESC, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        actor,
        ticket_count,
        ticket_count_2_rank,
        second_total_ticket_requester_id,
        DENSE_RANK() OVER (PARTITION BY prev_ticket_count, ticket_agent_id ORDER BY prev_ticket_count ASC, id) AS prev_ticket_count_dense_rank,
        MIN(prev_ticket_count) OVER (PARTITION BY actor, ticket_id ORDER BY prev_ticket_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_prev_ticket_count_so_far,
        RANK() OVER w_actor_ticket_id_by_prev_tick_2 AS prev_ticket_count_rank
    FROM windowed
    WINDOW w_actor_ticket_id_by_prev_tick_2 AS (PARTITION BY actor, ticket_id ORDER BY prev_ticket_count NULLS LAST)
)
SELECT id, metric, value
FROM (
    SELECT id, prev_ticket_count_rank, ticket_count_2_rank
    FROM ranked
) src
UNPIVOT (
    value FOR metric IN (prev_ticket_count_rank AS prev_ticket_count_rank_v, ticket_count_2_rank AS ticket_count_2_rank_v)
) unp
ORDER BY id, metric
