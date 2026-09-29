WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix_2,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE channel = 'email'
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team, su.hired_on AS agent_hired_on
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM combined_details_agent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'customer'
        )
),
unique_rows AS (
    -- Keep the latest row per agent_id, channel
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        priority_prefix,
        priority_prefix_2,
        requester_id_bucket,
        agent_team,
        agent_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY csat DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.priority_prefix_2,
        s.agent_team,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM unique_rows s
    JOIN ticket_events t ON t.ticket_id = s.id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM exploded_detail_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
deduped AS (
    -- Keep the earliest row per agent_id, agent_team
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        priority_prefix_2,
        agent_team,
        event_id,
        event_type,
        event_actor,
        last_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, agent_team ORDER BY opened_at, id, event_id) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    agent_id,
    opened_at,
    channel,
    csat,
    agent_team,
    event_id,
    event_type,
    event_actor,
    last_event_at,
    LOWER(REGEXP_REPLACE(TRIM(priority_prefix_2), ' ', '_')) AS priority_prefix_2_slug,
    CONCAT(agent_team, '#', CAST(id AS STRING)) AS agent_team_ref
FROM deduped
ORDER BY event_actor DESC, opened_at, id, event_id
LIMIT 5
