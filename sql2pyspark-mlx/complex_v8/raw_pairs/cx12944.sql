WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        LENGTH(TRIM(event_type)) AS event_type_len_2
    FROM ticket_events
    WHERE (HOUR(event_at) BETWEEN 8 AND 12 OR actor = 'system')
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM base_events s
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_len_2,
        first_event_at,
        LENGTH(TRIM(actor)) AS actor_len,
        ROUND(event_type_len / NULLIF(event_type_len_2, 0), 3) AS event_type_len_per_event_type_len_2
    FROM summarized_rows
),
event_type_stats AS (
    -- Max event_type_len_2 per event_type
    SELECT event_type, MAX(event_type_len_2) AS max_event_type_len_2
    FROM tagged
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_type_len_2,
        ROUND(s.event_type_len_2 - g.max_event_type_len_2, 2) AS event_type_len_2_minus_max
    FROM tagged s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_type_len_2 = g.max_event_type_len_2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_len_2,
        first_event_at,
        event_type_len_per_event_type_len_2,
        max_event_type_len_2,
        event_type_len_2_minus_max,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_type_len_per_event_type_len_2 DESC, id), 3) AS event_type_len_per_event_type_len_2_percent_rank,
        NTH_VALUE(actor_len, 2) OVER (ORDER BY event_type_len_per_event_type_len_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_actor_len,
        ROUND(actor_len - AVG(actor_len) OVER (PARTITION BY event_type_len_2, event_type), 2) AS actor_len_vs_event_type_len_2_event_type_avg
    FROM vs_group
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.first_event_at,
        s.event_type_len_per_event_type_len_2,
        s.max_event_type_len_2,
        s.event_type_len_2_minus_max,
        s.event_type_len_per_event_type_len_2_percent_rank,
        s.second_actor_len,
        t.closed_at AS ticket_closed_at,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM with_trends s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    first_event_at,
    event_type_len_per_event_type_len_2,
    max_event_type_len_2,
    event_type_len_2_minus_max,
    ticket_closed_at,
    agent_hired_on,
    ADD_MONTHS(agent_hired_on, 6) AS agent_hired_on_plus_6m,
    CASE WHEN agent_tier IN ('T2', 'T3') THEN 'group_a' WHEN agent_tier = 'T1' THEN 'group_b' ELSE 'other' END AS agent_tier_group
FROM joined_ticket
ORDER BY agent_tier_group DESC, agent_hired_on DESC
