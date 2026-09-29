WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 9 AND 13
      AND event_type IN ('reopened', 'replied')
),
latest_per_key AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id DESC) AS rn
        FROM filtered_events
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_event_at_to_date_by_event_at, 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM latest_per_key
    WINDOW w_event_at_to_date_by_event_at AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC NULLS FIRST)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_percent_rank,
        s.last_known_event_at,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at,
        t.first_event_at
    FROM enriched s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.last_known_event_at,
        s.ticket_requester_id,
        s.agent_team,
        s.event_count,
        s.last_event_at,
        s.first_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM profiled s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
grouped AS (
    -- Summarise per group
    SELECT
        TO_DATE(event_at) AS event_at_day,
        YEAR(last_known_event_at) AS last_known_event_at_year,
        ticket_id,
        MIN(first_event_at) AS first_first_event_at,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        SUM(CASE WHEN event_actor NOT IN ('system', 'customer') THEN 1 ELSE 0 END) AS n_event_actor_not_in_system_cu
    FROM exploded_detail
    WHERE event_actor IN ('system', 'agent')
    GROUP BY TO_DATE(event_at), YEAR(last_known_event_at), ticket_id
    HAVING AVG(event_count) > 1
)
-- Final projection
SELECT
    event_at_day,
    last_known_event_at_year,
    first_first_event_at,
    agent_team_list,
    n_event_actor_not_in_system_cu,
    QUARTER(event_at_day) AS event_at_day_quarter,
    ABS(last_known_event_at_year - 2025) AS last_known_event_at_year_distance
FROM grouped
