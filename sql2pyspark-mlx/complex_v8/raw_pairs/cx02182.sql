WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 6 AND 8
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_all_by_event_at AS event_at_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM base
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at_rank, 1, 0) OVER (PARTITION BY event_at_rank, actor_group ORDER BY event_at ASC NULLS LAST, id) AS next_event_at_rank,
        ROUND(AVG(event_at_rank) OVER w_actor_group_ticket_id, 2) AS actor_group_ticket_id_avg_event_at_rank,
        DENSE_RANK() OVER (PARTITION BY event_at_rank, actor_group ORDER BY event_at ASC NULLS LAST) AS event_at_dense_rank,
        SUM(event_at_rank) OVER (PARTITION BY event_at_rank, actor_group ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_rank_sum_3
    FROM metrics
    WINDOW w_actor_group_ticket_id AS (PARTITION BY actor_group, ticket_id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_event_at_rank_by_event_at_de, 3) AS event_at_dense_rank_percent_rank,
        ROUND(event_at_rank_sum_3 - AVG(event_at_rank_sum_3) OVER (PARTITION BY actor, actor_group), 2) AS event_at_rank_sum_3_vs_actor_actor_group_avg
    FROM with_trends
    WINDOW w_event_at_rank_by_event_at_de AS (PARTITION BY event_at_rank ORDER BY event_at_dense_rank)
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.requester_id <= 23
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor_group,
        event_at_rank,
        first_known_event_at,
        actor_group_ticket_id_avg_event_at_rank,
        event_at_rank_sum_3_vs_actor_actor_group_avg,
        LAG(event_at_dense_rank) OVER (PARTITION BY actor_group ORDER BY next_event_at_rank, id) AS prev_event_at_dense_rank,
        MIN(event_at_rank_sum_3) OVER (PARTITION BY actor_group, actor ORDER BY event_at_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_event_at_rank_sum_3_so_far,
        COUNT(*) OVER w_event_at_rank_event_at_to_da AS rows_in_event_at_rank_event_at_to_date,
        ROUND(CUME_DIST() OVER (PARTITION BY actor_group, actor ORDER BY event_at_rank), 3) AS event_at_rank_cume_dist,
        ROUND(AVG(next_event_at_rank) OVER (ORDER BY event_at_dense_rank_percent_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS next_event_at_rank_avg_4,
        ROUND(AVG(next_event_at_rank) OVER (PARTITION BY actor_group ORDER BY next_event_at_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS next_event_at_rank_avg_4_2
    FROM qualified
    WINDOW w_event_at_rank_event_at_to_da AS (PARTITION BY event_at_rank, TO_DATE(event_at))
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_group,
        s.event_at_rank,
        s.actor_group_ticket_id_avg_event_at_rank,
        s.prev_event_at_dense_rank,
        s.event_at_rank_cume_dist,
        s.next_event_at_rank_avg_4_2,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.agent_name
    FROM sequenced s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id < 16
    JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    ticket_channel,
    agent_tier,
    actor_group,
    GROUPING_ID() AS grouping_level,
    MAX(event_at) AS last_event_at,
    SUM(actor_group_ticket_id_avg_event_at_rank) FILTER (WHERE ticket_channel IN ('phone', 'web')) AS actor_group_ticket_id_avg_event_at_rank_ticket_c,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM enriched_ticket
GROUP BY GROUPING SETS ((ticket_channel, agent_tier, actor_group), (ticket_channel))
ORDER BY ticket_channel NULLS LAST, agent_tier NULLS LAST, actor_group NULLS LAST
