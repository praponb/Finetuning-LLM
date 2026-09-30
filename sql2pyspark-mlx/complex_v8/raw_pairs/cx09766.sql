WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE actor IS NOT NULL
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(agent_hired_on) OVER (PARTITION BY event_type ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY agent_hired_on DESC, id) AS seq_no,
        RANK() OVER (ORDER BY event_at) AS event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(agent_hired_on) ORDER BY event_at ASC, id), 3) AS event_at_percent_rank,
        DENSE_RANK() OVER w_agent_hired_on_month_ticket AS event_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY agent_hired_on DESC) AS agent_hired_on_dense_rank,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), ticket_id ORDER BY event_at, id)) AS days_since_prev_agent_hired_on,
        RANK() OVER (PARTITION BY event_type ORDER BY agent_hired_on DESC) AS agent_hired_on_rank,
        LEAD(ticket_closed_at) OVER w_event_type_by_agent_hired_on_2 AS next_ticket_closed_at,
        ROUND(PERCENT_RANK() OVER w_agent_hired_on_month_by_even_2, 3) AS event_at_percent_rank_2
    FROM with_details_ticket
    WINDOW w_agent_hired_on_month_ticket AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), ticket_id ORDER BY event_at), w_event_type_by_agent_hired_on_2 AS (PARTITION BY event_type ORDER BY agent_hired_on DESC, id), w_agent_hired_on_month_by_even_2 AS (PARTITION BY MONTH(agent_hired_on) ORDER BY event_at ASC)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE event_at_rank <= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(event_at_percent_rank_2 - AVG(event_at_percent_rank_2) OVER w_event_at_dense_rank, 2) AS event_at_percent_rank_2_vs_event_at_dense_rank_a,
        DENSE_RANK() OVER w_next_ticket_closed_at_month AS agent_hired_on_rank_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at_dense_rank, id), 3) AS event_at_dense_rank_cume_dist,
        ROUND(event_at_dense_rank / NULLIF(SUM(event_at_dense_rank) OVER (PARTITION BY event_at_dense_rank), 0), 4) AS event_at_dense_rank_share_of_event_at_dense_rank,
        ROUND(event_at_rank - AVG(event_at_rank) OVER (PARTITION BY agent_hired_on_dense_rank), 2) AS event_at_rank_vs_agent_hired_on_dense_rank_avg
    FROM leaders
    WINDOW w_event_at_dense_rank AS (PARTITION BY event_at_dense_rank), w_next_ticket_closed_at_month AS (PARTITION BY MONTH(next_ticket_closed_at), event_at_dense_rank ORDER BY agent_hired_on_rank DESC, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_at_label,
    ticket_priority,
    ticket_closed_at,
    agent_name,
    agent_hired_on,
    seq_no,
    event_at_rank,
    event_at_dense_rank,
    agent_hired_on_dense_rank,
    days_since_prev_agent_hired_on,
    agent_hired_on_rank,
    next_ticket_closed_at,
    event_at_percent_rank_2,
    event_at_percent_rank_2_vs_event_at_dense_rank_a,
    agent_hired_on_rank_dense_rank,
    event_at_dense_rank_cume_dist,
    CONCAT(actor, '#', CAST(event_at_rank AS STRING)) AS actor_ref,
    DATEDIFF(agent_hired_on, last_agent_hired_on) AS days_last_agent_hired_on_to_agent_hired_on
FROM ranked
