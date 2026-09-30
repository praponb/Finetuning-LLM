WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-02'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority,
        su.agent_name
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_channel,
        ticket_priority,
        agent_name,
        DENSE_RANK() OVER (PARTITION BY ticket_channel, MONTH(event_at) ORDER BY event_at ASC NULLS LAST) AS event_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_channel, ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(5) OVER (PARTITION BY ticket_channel, MONTH(event_at) ORDER BY event_at ASC NULLS LAST, id) AS event_at_quintile,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(4) OVER (PARTITION BY ticket_priority, YEAR(event_at) ORDER BY event_at NULLS LAST, id) AS event_at_quartile,
        NTILE(10) OVER w_all_by_event_at AS event_at_decile,
        NTILE(4) OVER (PARTITION BY ticket_channel, ticket_id ORDER BY event_at, id) AS event_at_quartile_2
    FROM with_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(event_at_quartile) OVER (PARTITION BY ticket_id ORDER BY second_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_quartile,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_second_event_at,
        SIZE(COLLECT_SET(actor) OVER w_event_at_decile_ticket_prior) AS distinct_actor_in_event_at_decile_ticket_priorit,
        NTH_VALUE(event_at_dense_rank, 2) OVER (PARTITION BY event_at_quartile_2, event_at_decile ORDER BY event_at_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_dense_rank,
        ROUND(AVG(event_at_decile) OVER (ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS event_at_decile_avg_3_prior,
        COUNT(*) OVER (PARTITION BY event_at_decile, ticket_priority) AS rows_in_event_at_decile_ticket_priority,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_at_decile, ticket_priority)) AS distinct_ticket_id_in_event_at_decile_ticket_pri
    FROM windowed
    WINDOW w_event_at_decile_ticket_prior AS (PARTITION BY event_at_decile, ticket_priority)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_ticket_id_in_event_at_decile_ticket_pri,
        event_at_dense_rank,
        event_at_quartile,
        MIN(second_event_at_dense_rank) AS min_second_event_at_dense_rank,
        MIN(second_event_at) AS first_second_event_at,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list
    FROM windowed_2
    GROUP BY distinct_ticket_id_in_event_at_decile_ticket_pri, event_at_dense_rank, event_at_quartile
    HAVING SUM(event_at_decile_avg_3_prior) > 4.6
)
SELECT
    distinct_ticket_id_in_event_at_decile_ticket_pri,
    event_at_dense_rank,
    event_at_quartile,
    min_second_event_at_dense_rank,
    first_second_event_at,
    distinct_ids,
    actor_list,
    ABS(event_at_dense_rank - 5) AS event_at_dense_rank_distance
FROM stats
ORDER BY event_at_dense_rank_distance
