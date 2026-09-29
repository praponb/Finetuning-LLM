WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_type = 'assigned'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        DENSE_RANK() OVER w_all_by_event_at AS event_at_dense_rank_2,
        DENSE_RANK() OVER w_ticket_priority_actor_by_tic AS ticket_closed_at_dense_rank,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        NTILE(10) OVER w_event_type_actor_by_ticket_c AS ticket_closed_at_decile,
        ROW_NUMBER() OVER w_ticket_priority_actor_by_tic AS ticket_closed_at_seq,
        LAG(ticket_closed_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS prev2_ticket_closed_at,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        LAST_VALUE(ticket_closed_at) OVER (PARTITION BY ticket_priority, actor ORDER BY ticket_closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id), 3) AS event_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_ties, 3) AS event_at_percent_rank,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS prev2_event_at
    FROM combined_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id), w_ticket_priority_actor_by_tic AS (PARTITION BY ticket_priority, actor ORDER BY ticket_closed_at NULLS LAST, id), w_all_by_event_at_ties AS (ORDER BY event_at DESC), w_event_type_actor_by_ticket_c AS (PARTITION BY event_type, actor ORDER BY ticket_closed_at DESC, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE ticket_closed_at_dense_rank <= 3
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        ticket_agent_id,
        event_at_dense_rank,
        ticket_closed_at_dense_rank,
        event_at_cume_dist,
        ticket_closed_at_decile,
        ticket_closed_at_seq,
        prev2_ticket_closed_at,
        minutes_since_prev_ticket_closed_at,
        seq_no,
        event_at_cume_dist_2,
        event_at_percent_rank,
        second_event_at,
        ticket_closed_at_decile - LAG(ticket_closed_at_decile) OVER w_event_at_dense_rank_2_event AS ticket_closed_at_decile_change,
        COUNT(*) OVER w_ticket_closed_at_seq_event_a AS rows_in_ticket_closed_at_seq_event_at_dense_rank,
        SIZE(COLLECT_SET(event_type) OVER w_ticket_closed_at_seq_event_a) AS distinct_event_type_in_ticket_closed_at_seq_even,
        ROUND(AVG(event_at_dense_rank) OVER (PARTITION BY ticket_closed_at_seq, event_at_dense_rank), 2) AS ticket_closed_at_seq_event_at_dense_rank_avg_eve,
        ROUND(PERCENT_RANK() OVER w_all_by_seq_no_ties, 3) AS seq_no_percent_rank
    FROM top_rows
    WINDOW w_event_at_dense_rank_2_event AS (PARTITION BY event_at_dense_rank_2, event_type ORDER BY event_at_cume_dist, id), w_ticket_closed_at_seq_event_a AS (PARTITION BY ticket_closed_at_seq, event_at_dense_rank), w_all_by_seq_no_ties AS (ORDER BY seq_no DESC NULLS FIRST)
),
summary AS (
    -- Summarise per group
    SELECT
        ticket_closed_at_dense_rank,
        SUM(CASE WHEN actor <> 'agent' THEN ticket_closed_at_decile_change ELSE 0 END) AS ticket_closed_at_decile_change_actor_agent,
        ROUND(STDDEV(ticket_closed_at_seq_event_at_dense_rank_avg_eve), 2) AS stddev_ticket_closed_at_seq_event_at_dense_rank_
    FROM metrics
    WHERE seq_no < 8
    GROUP BY ticket_closed_at_dense_rank
),
windowed AS (
    -- Previous / next values per key
    SELECT
        ticket_closed_at_dense_rank,
        ticket_closed_at_decile_change_actor_agent,
        LAST_VALUE(ticket_closed_at_dense_rank) IGNORE NULLS OVER (PARTITION BY ticket_closed_at_dense_rank ORDER BY ticket_closed_at_decile_change_actor_agent, ticket_closed_at_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_closed_at_dense_rank,
        NTH_VALUE(ticket_closed_at_dense_rank, 2) OVER (PARTITION BY ticket_closed_at_dense_rank ORDER BY ticket_closed_at_decile_change_actor_agent, ticket_closed_at_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at_dense_rank
    FROM summary
)
-- Final projection
SELECT
    ticket_closed_at_dense_rank,
    ticket_closed_at_decile_change_actor_agent,
    last_known_ticket_closed_at_dense_rank,
    second_ticket_closed_at_dense_rank
FROM windowed
ORDER BY ticket_closed_at_decile_change_actor_agent NULLS LAST, second_ticket_closed_at_dense_rank DESC, ticket_closed_at_dense_rank
LIMIT 10
