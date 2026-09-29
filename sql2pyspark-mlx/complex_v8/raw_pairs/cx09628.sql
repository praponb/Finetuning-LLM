WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
joined AS (
    -- Join tickets
    SELECT
        s.*,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id <= 22
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_year,
        ticket_tags,
        ticket_requester_id,
        agent_tier,
        agent_team,
        ROUND(AVG(ticket_requester_id) OVER w_event_type, 2) AS event_type_avg_ticket_requester_id,
        LAG(ticket_requester_id) OVER (PARTITION BY ticket_id, event_at_year ORDER BY event_at DESC NULLS FIRST, id) AS prev_ticket_requester_id,
        DENSE_RANK() OVER w_ticket_id_event_at_year_by_e AS event_at_dense_rank,
        RANK() OVER w_all_by_event_at_year AS event_at_year_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_at_year ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ticket_requester_id - LAG(ticket_requester_id) OVER w_ticket_id_event_at_year_by_e AS ticket_requester_id_change,
        COUNT(*) OVER (PARTITION BY ticket_id, agent_team) AS rows_in_ticket_id_agent_team,
        LEAD(ticket_requester_id, 1, 0) OVER (PARTITION BY agent_team, actor ORDER BY event_at DESC, id) AS next_ticket_requester_id,
        ROW_NUMBER() OVER (ORDER BY event_at_year ASC, id) AS event_at_year_seq
    FROM joined
    WINDOW w_event_type AS (PARTITION BY event_type), w_ticket_id_event_at_year_by_e AS (PARTITION BY ticket_id, event_at_year ORDER BY event_at DESC NULLS FIRST, id), w_all_by_event_at_year AS (ORDER BY event_at_year ASC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_year,
        ticket_tags,
        ticket_requester_id,
        agent_tier,
        agent_team,
        event_type_avg_ticket_requester_id,
        prev_ticket_requester_id,
        event_at_dense_rank,
        first_known_event_at,
        ticket_requester_id_change,
        rows_in_ticket_id_agent_team,
        next_ticket_requester_id,
        event_at_year_seq
    FROM metrics
    WHERE event_at_year_rank <= 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_tags,
        ticket_requester_id,
        agent_team,
        event_type_avg_ticket_requester_id,
        prev_ticket_requester_id,
        event_at_dense_rank,
        ticket_requester_id_change,
        rows_in_ticket_id_agent_team,
        next_ticket_requester_id,
        event_at_year_seq,
        ROW_NUMBER() OVER w_all_by_ticket_requester_id_c AS ticket_requester_id_change_seq,
        DENSE_RANK() OVER (ORDER BY ticket_requester_id_change) AS ticket_requester_id_change_dense_rank,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_year)) AS distinct_actor_in_event_at_year,
        LEAD(next_ticket_requester_id) OVER (ORDER BY ticket_requester_id_change, id) AS next_next_ticket_requester_id,
        MAX(prev_ticket_requester_id) OVER (PARTITION BY event_at_year ORDER BY event_at_year_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_ticket_requester_id_max_6_prior,
        ROUND(event_at_year - AVG(event_at_year) OVER (PARTITION BY event_at_year), 2) AS event_at_year_vs_event_at_year_avg,
        prev_ticket_requester_id - LAG(prev_ticket_requester_id) OVER (ORDER BY ticket_requester_id_change, id) AS prev_ticket_requester_id_change,
        COUNT(*) OVER w_event_at_year AS rows_in_event_at_year
    FROM leaders
    WINDOW w_all_by_ticket_requester_id_c AS (ORDER BY ticket_requester_id_change, id), w_event_at_year AS (PARTITION BY event_at_year)
)
-- Final projection
SELECT
    id,
    actor,
    ticket_tags,
    agent_team,
    event_type_avg_ticket_requester_id,
    prev_ticket_requester_id,
    ticket_requester_id_change,
    next_ticket_requester_id,
    event_at_year_seq,
    ticket_requester_id_change_seq,
    ticket_requester_id_change_dense_rank,
    distinct_actor_in_event_at_year,
    next_next_ticket_requester_id,
    event_at_year_vs_event_at_year_avg,
    prev_ticket_requester_id_change,
    rows_in_event_at_year,
    DATE_TRUNC('HOUR', event_at) AS event_at_hour,
    TO_DATE(event_at) AS event_at_date
FROM with_windows
ORDER BY ticket_requester_id_change, id
LIMIT 10
