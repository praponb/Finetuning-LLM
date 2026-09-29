WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'created'
      AND actor IS NOT NULL
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY ticket_csat) AS ticket_csat_dense_rank,
        ROUND(ticket_csat - AVG(ticket_csat) OVER w_agent_hired_on_year_ticket_i, 2) AS ticket_csat_vs_agent_hired_on_year_ticket_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_requester_id DESC), 3) AS ticket_requester_id_cume_dist,
        COUNT(*) OVER (PARTITION BY YEAR(agent_hired_on), ticket_id) AS rows_in_agent_hired_on_year_ticket_id,
        SIZE(COLLECT_SET(ticket_priority) OVER w_agent_hired_on_year_ticket_i) AS distinct_ticket_priority_in_agent_hired_on_year_,
        ROUND(PERCENT_RANK() OVER w_all_by_ticket_requester_id_t, 3) AS ticket_requester_id_percent_rank,
        ROUND(AVG(ticket_csat) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_6_prior,
        LEAD(ticket_csat, 2) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY ticket_csat, id) AS next2_ticket_csat,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY YEAR(agent_hired_on), ticket_id), 0), 4) AS ticket_requester_id_share_of_agent_hired_on_year,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(ticket_csat, 3) OVER (ORDER BY agent_hired_on, id) AS next3_ticket_csat
    FROM joined_ticket
    WINDOW w_agent_hired_on_year_ticket_i AS (PARTITION BY YEAR(agent_hired_on), ticket_id), w_all_by_ticket_requester_id_t AS (ORDER BY ticket_requester_id DESC)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE ticket_csat_dense_rank <= 5
      AND event_at >= TIMESTAMP '2025-01-25 09:00:00'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(ticket_csat_avg_6_prior) OVER w_event_type_next3_ticket_csat AS event_type_next3_ticket_csat_sum_ticket_csat_avg,
        ROUND(CUME_DIST() OVER w_actor_by_event_at_ties, 3) AS event_at_cume_dist,
        ROUND(AVG(ticket_csat_dense_rank) OVER (PARTITION BY distinct_ticket_priority_in_agent_hired_on_year_ ORDER BY ticket_csat_vs_agent_hired_on_year_ticket_id_avg DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_dense_rank_avg_2,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY event_type, next3_ticket_csat), 2) AS ticket_requester_id_vs_event_type_next3_ticket_c,
        SUM(next2_ticket_csat) OVER (PARTITION BY distinct_ticket_priority_in_agent_hired_on_year_ ORDER BY ticket_csat_vs_agent_hired_on_year_ticket_id_avg DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next2_ticket_csat_sum_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_ticket_priority_in_agent_hired_on_year_ ORDER BY ticket_csat_vs_agent_hired_on_year_ticket_id_avg DESC), 3) AS ticket_csat_vs_agent_hired_on_year_ticket_id_avg_2,
        ROUND(CUME_DIST() OVER w_actor_by_event_at, 3) AS event_at_cume_dist_2,
        FIRST_VALUE(next2_ticket_csat) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next2_ticket_csat
    FROM first_rows
    WINDOW w_event_type_next3_ticket_csat AS (PARTITION BY event_type, next3_ticket_csat), w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at DESC), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    ticket_requester_id,
    ticket_priority,
    agent_name,
    ticket_csat_vs_agent_hired_on_year_ticket_id_avg,
    ticket_requester_id_cume_dist,
    rows_in_agent_hired_on_year_ticket_id,
    ticket_requester_id_percent_rank,
    ticket_csat_avg_6_prior,
    next2_ticket_csat,
    ticket_requester_id_share_of_agent_hired_on_year,
    first_known_event_at,
    next3_ticket_csat,
    event_type_next3_ticket_csat_sum_ticket_csat_avg,
    event_at_cume_dist,
    ticket_csat_dense_rank_avg_2,
    next2_ticket_csat_sum_2_prior,
    event_at_cume_dist_2,
    first_next2_ticket_csat
FROM metrics
WHERE ticket_id >= 27
ORDER BY event_at_cume_dist_2, ticket_requester_id_cume_dist
