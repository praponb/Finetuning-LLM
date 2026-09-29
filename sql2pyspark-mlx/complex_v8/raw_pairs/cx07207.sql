WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        tags
    FROM tickets
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        su.tier AS agent_tier,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'H%'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS event_count,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        t.first_event_at_2
    FROM combined_details s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
grouped AS (
    -- Summarise per group
    SELECT
        event_count,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(*) AS row_count,
        SUM(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'F%' THEN 1 ELSE 0 END) AS n_upper_trim_agent_name_like_f,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        ROUND(STDDEV(event_count), 2) AS stddev_event_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_counts
    GROUP BY event_count
    HAVING AVG(event_count) > 3
)
SELECT
    event_count,
    agent_name_list,
    row_count,
    n_upper_trim_agent_name_like_f,
    distinct_agent_ids,
    distinct_ids + row_count AS distinct_ids_plus_row_count
FROM grouped
ORDER BY event_count, row_count DESC
