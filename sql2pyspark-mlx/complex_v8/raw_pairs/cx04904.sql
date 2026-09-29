WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER w_channel) AS distinct_priority_in_channel,
        MIN(requester_id) OVER w_channel AS channel_min_requester_id,
        MIN(csat) OVER (PARTITION BY channel) AS channel_min_csat
    FROM filtered_tickets
    WINDOW w_channel AS (PARTITION BY channel)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        GREATEST(channel_min_requester_id, requester_id_per_csat) AS greatest_channel_min_requester_id_requester_id_p
    FROM with_windows
    WHERE closed_at < TIMESTAMP '2025-02-20 15:00:00'
),
unique_rows AS (
    -- Keep the latest row per priority
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        requester_id_per_csat,
        distinct_priority_in_channel,
        channel_min_requester_id,
        channel_min_csat,
        opened_at_label,
        priority_slug,
        greatest_channel_min_requester_id_requester_id_p
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY distinct_priority_in_channel DESC, id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.requester_id_per_csat,
        s.channel_min_requester_id,
        s.channel_min_csat,
        s.opened_at_label,
        su.agent_name,
        su.team AS agent_team
    FROM unique_rows s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        LEAD(requester_id) OVER (PARTITION BY priority ORDER BY requester_id_per_csat DESC, id) AS next_requester_id,
        SIZE(COLLECT_SET(agent_team) OVER w_priority) AS distinct_agent_team_in_priority,
        LEAD(channel_min_csat, 1, 0) OVER (PARTITION BY agent_team ORDER BY channel_min_csat DESC, id) AS next_channel_min_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY priority), 0), 4) AS csat_share_of_priority,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority
    FROM with_details
    WINDOW w_priority AS (PARTITION BY priority)
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    requester_id_per_csat,
    channel_min_requester_id,
    channel_min_csat,
    agent_team,
    last_known_csat,
    next_requester_id,
    distinct_agent_team_in_priority,
    next_channel_min_csat,
    csat_share_of_priority,
    distinct_agent_id_in_priority,
    LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
    GREATEST(requester_id, csat) AS greatest_requester_id_csat
FROM with_windows_2
ORDER BY id NULLS LAST, agent_id DESC NULLS FIRST
LIMIT 10
