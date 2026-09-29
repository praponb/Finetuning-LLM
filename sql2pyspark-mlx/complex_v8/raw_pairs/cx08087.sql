WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_type NOT IN ('created', 'replied')
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_clean,
        s.event_at_date,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat,
        su.tier AS agent_tier
    FROM recent_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        event_at_date,
        ticket_closed_at,
        ticket_agent_id,
        ticket_csat,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY ticket_csat, ticket_agent_id ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        LAST_VALUE(event_at_date) IGNORE NULLS OVER (PARTITION BY ticket_csat, ticket_agent_id ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_date,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY TO_DATE(event_at), agent_tier), 0), 4) AS ticket_csat_share_of_event_at_to_date_agent_tier,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_csat ORDER BY event_at_date, id), 3) AS event_at_date_percent_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(event_at), agent_tier) AS rows_in_event_at_to_date_agent_tier,
        NTH_VALUE(ticket_csat, 2) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat,
        LAST_VALUE(event_at_date) OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_date,
        SUM(ticket_csat) OVER w_ticket_csat AS ticket_csat_sum_ticket_csat,
        FIRST_VALUE(event_at_date) IGNORE NULLS OVER (PARTITION BY actor ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_date,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER w_ticket_csat_by_event_at_date)) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER w_actor_by_event_at AS event_at_rank,
        RANK() OVER w_ticket_csat_ticket_agent_id_2 AS ticket_closed_at_rank,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_csat), 0), 4) AS ticket_csat_share_of_ticket_csat
    FROM with_parent_ticket
    WINDOW w_ticket_csat AS (PARTITION BY ticket_csat), w_ticket_csat_by_event_at_date AS (PARTITION BY ticket_csat ORDER BY event_at_date, id), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at ASC, id), w_ticket_csat_ticket_agent_id_2 AS (PARTITION BY ticket_csat, ticket_agent_id ORDER BY ticket_closed_at DESC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        first_event_at + INTERVAL 30 MINUTES AS first_event_at_plus_30m,
        DAYOFWEEK(last_event_at_date) AS last_event_at_date_dayofweek,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM with_trends
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        actor,
        ticket_closed_at,
        ticket_agent_id,
        ticket_csat,
        last_known_event_at_date,
        event_at_date_percent_rank,
        rows_in_event_at_to_date_agent_tier,
        second_ticket_csat,
        ticket_csat_sum_ticket_csat,
        first_known_event_at_date,
        minutes_since_prev_ticket_closed_at,
        first_event_at,
        ticket_closed_at_rank,
        ticket_csat_share_of_ticket_csat,
        first_event_at_plus_30m,
        last_event_at_date_dayofweek,
        actor_prefix,
        DENSE_RANK() OVER (PARTITION BY MONTH(last_event_at_date), actor ORDER BY ticket_csat_share_of_ticket_csat ASC) AS ticket_csat_share_of_ticket_csat_dense_rank,
        SUM(ticket_csat) OVER (PARTITION BY second_ticket_csat, actor) AS second_ticket_csat_actor_sum_ticket_csat,
        COUNT(*) OVER w_second_ticket_csat_actor AS rows_in_second_ticket_csat_actor,
        LEAD(ticket_csat, 3) OVER w_all_by_event_at_date AS next3_ticket_csat
    FROM labeled
    WINDOW w_second_ticket_csat_actor AS (PARTITION BY second_ticket_csat, actor), w_all_by_event_at_date AS (ORDER BY event_at_date, id)
)
SELECT
    id,
    actor,
    ticket_agent_id,
    ticket_csat,
    last_known_event_at_date,
    event_at_date_percent_rank,
    second_ticket_csat,
    ticket_csat_sum_ticket_csat,
    first_known_event_at_date,
    minutes_since_prev_ticket_closed_at,
    first_event_at,
    ticket_closed_at_rank,
    ticket_csat_share_of_ticket_csat,
    first_event_at_plus_30m,
    last_event_at_date_dayofweek,
    actor_prefix,
    second_ticket_csat_actor_sum_ticket_csat,
    rows_in_second_ticket_csat_actor,
    next3_ticket_csat,
    ROUND(second_ticket_csat / NULLIF(ticket_closed_at_rank, 0), 3) AS second_ticket_csat_per_ticket_closed_at_rank,
    CONCAT_WS(': ', actor, actor_prefix) AS actor_actor_prefix_label
FROM with_trends_2
