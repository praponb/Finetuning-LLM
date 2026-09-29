WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE event_type = 'replied'
      AND ticket_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_year,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        su.agent_name
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.opened_at >= TIMESTAMP '2025-01-13 09:00:00'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_opened_at,
        ticket_agent_id,
        agent_name,
        DENSE_RANK() OVER w_all_by_event_at_year_ties AS event_at_year_dense_rank,
        event_at_year - LAG(event_at_year) OVER w_all_by_event_at_year AS event_at_year_change,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at), event_at_year)) AS distinct_actor_in_ticket_opened_at_month_event_a,
        RANK() OVER (ORDER BY event_at_year, id) AS event_at_year_rank,
        COUNT(*) OVER w_event_at_year_ticket_agent_i AS rows_in_event_at_year_ticket_agent_id,
        MIN(event_at_year) OVER w_ticket_opened_at_month_event AS ticket_opened_at_month_event_at_year_min_event_a,
        SIZE(COLLECT_SET(actor) OVER w_event_at_year_ticket_agent_i) AS distinct_actor_in_event_at_year_ticket_agent_id,
        LEAD(event_at_year) OVER (ORDER BY event_at_year, id) AS next_event_at_year,
        COUNT(*) OVER (ORDER BY event_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(event_at_year, 1, 0) OVER (PARTITION BY actor, event_at_year ORDER BY ticket_opened_at, id) AS next_event_at_year_2
    FROM with_details
    WINDOW w_all_by_event_at_year_ties AS (ORDER BY event_at_year), w_all_by_event_at_year AS (ORDER BY event_at_year, id), w_event_at_year_ticket_agent_i AS (PARTITION BY event_at_year, ticket_agent_id), w_ticket_opened_at_month_event AS (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at), event_at_year)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_opened_at,
        agent_name,
        event_at_year_dense_rank,
        event_at_year_change,
        event_at_year_rank,
        rows_in_event_at_year_ticket_agent_id,
        ticket_opened_at_month_event_at_year_min_event_a,
        distinct_actor_in_event_at_year_ticket_agent_id,
        running_count,
        next_event_at_year_2,
        ROUND(event_at_year_dense_rank / NULLIF(SUM(event_at_year_dense_rank) OVER (PARTITION BY ticket_agent_id, actor), 0), 4) AS event_at_year_dense_rank_share_of_ticket_agent_i,
        LAG(event_at, 2) OVER (PARTITION BY actor, ticket_agent_id ORDER BY distinct_actor_in_ticket_opened_at_month_event_a NULLS LAST, id) AS prev2_event_at,
        COUNT(*) OVER (PARTITION BY ticket_agent_id, actor) AS rows_in_ticket_agent_id_actor,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER w_next_event_at_year_2_by_tick)) / 60, 1) AS minutes_since_prev_ticket_opened_at,
        NTILE(10) OVER w_all_by_ticket_opened_at AS ticket_opened_at_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY next_event_at_year_2 ORDER BY ticket_opened_at NULLS LAST, id), 3) AS ticket_opened_at_cume_dist
    FROM sequenced
    WINDOW w_next_event_at_year_2_by_tick AS (PARTITION BY next_event_at_year_2 ORDER BY ticket_opened_at NULLS LAST, id), w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at, id)
)
SELECT
    id,
    event_at,
    actor,
    ticket_opened_at,
    agent_name,
    event_at_year_dense_rank,
    event_at_year_change,
    event_at_year_rank,
    rows_in_event_at_year_ticket_agent_id,
    ticket_opened_at_month_event_at_year_min_event_a,
    distinct_actor_in_event_at_year_ticket_agent_id,
    running_count,
    next_event_at_year_2,
    prev2_event_at,
    minutes_since_prev_ticket_opened_at,
    ticket_opened_at_cume_dist,
    CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
    ROUND(ticket_opened_at_decile / NULLIF(event_at_year_dense_rank, 0), 3) AS ticket_opened_at_decile_per_event_at_year_dense_
FROM sequenced_2
ORDER BY actor, ticket_opened_at_decile_per_event_at_year_dense_ DESC NULLS FIRST
