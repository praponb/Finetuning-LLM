WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE (event_type IN ('resolved', 'replied', 'escalated') OR ticket_id IS NOT NULL)
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_clean,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        su.team AS agent_team
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LEAD(event_at, 2) OVER (ORDER BY event_at NULLS LAST, id) AS next2_event_at,
        LAG(event_at, 3) OVER w_ticket_id_by_event_at AS prev3_event_at,
        NTILE(4) OVER w_ticket_id_by_event_at_2 AS event_at_quartile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at_2)) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(10) OVER w_ticket_id_by_event_at AS event_at_decile,
        ROUND(CUME_DIST() OVER (ORDER BY event_at), 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at, 3) AS event_at_cume_dist_2,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM combined_details_ticket
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at ASC, id), w_ticket_id_by_event_at_2 AS (PARTITION BY ticket_id ORDER BY event_at DESC, id), w_all_by_event_at_2 AS (ORDER BY event_at, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        event_type,
        actor_clean,
        ticket_channel,
        ticket_agent_id,
        agent_team,
        next2_event_at,
        prev3_event_at,
        event_at_quartile,
        event_at_cume_dist,
        event_at_cume_dist_2,
        second_event_at,
        first_known_event_at,
        event_at_percent_rank,
        last_known_event_at,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        CONCAT(ticket_channel, '#', CAST(id AS STRING)) AS ticket_channel_ref
    FROM metrics
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        actor_clean,
        ticket_channel,
        ticket_agent_id,
        agent_team,
        next2_event_at,
        event_at_cume_dist,
        event_at_cume_dist_2,
        second_event_at,
        first_known_event_at,
        event_at_percent_rank,
        last_known_event_at,
        event_type_ref,
        RANK() OVER w_ticket_agent_id_by_second_ev AS second_event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_channel ORDER BY second_event_at DESC), 3) AS second_event_at_cume_dist,
        COUNT(*) OVER w_last_known_event_at_year_age AS rows_in_last_known_event_at_year_agent_team,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_agent_id, event_at_quartile ORDER BY second_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2,
        LAST_VALUE(event_at_cume_dist) IGNORE NULLS OVER (PARTITION BY ticket_agent_id, event_at_quartile ORDER BY second_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_cume_dist,
        ROUND(event_at_quartile / NULLIF(SUM(event_at_quartile) OVER (PARTITION BY YEAR(last_known_event_at), agent_team), 0), 4) AS event_at_quartile_share_of_last_known_event_at_y
    FROM labeled
    WINDOW w_ticket_agent_id_by_second_ev AS (PARTITION BY ticket_agent_id ORDER BY second_event_at), w_last_known_event_at_year_age AS (PARTITION BY YEAR(last_known_event_at), agent_team)
)
SELECT
    id,
    actor_clean,
    agent_team,
    next2_event_at,
    event_at_cume_dist,
    event_at_cume_dist_2,
    first_known_event_at,
    last_known_event_at,
    event_type_ref,
    second_event_at_rank,
    rows_in_last_known_event_at_year_agent_team,
    first_known_event_at_2,
    last_known_event_at_cume_dist,
    event_at_quartile_share_of_last_known_event_at_y
FROM windowed
WHERE last_known_event_at_cume_dist < 0.6
ORDER BY next2_event_at NULLS LAST, id
LIMIT 20
