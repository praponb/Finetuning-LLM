WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.agent_id,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM matched s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.agent_id,
        s.ticket_tags,
        s.ticket_channel,
        s.agent_hired_on,
        s.agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on_2
    FROM combined_details s
    JOIN support_agents su ON su.id = s.agent_id
),
totals AS (
    -- Summarise per group
    SELECT
        agent_team,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(agent_name)) >= 5 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_agent_name_5,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        SUM(CASE WHEN LOWER(agent_name) LIKE '%ak%' THEN 1 ELSE 0 END) AS n_lower_agent_name_like_ak,
        DATEDIFF(MAX(agent_hired_on), MIN(agent_hired_on)) AS agent_hired_on_span_days,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(agent_hired_on) AS last_agent_hired_on
    FROM enriched_agent
    GROUP BY agent_team
)
-- Final projection
SELECT
    agent_team,
    pct_length_trim_agent_name_5,
    distinct_agent_ids,
    n_lower_agent_name_like_ak,
    agent_hired_on_span_days,
    last_agent_hired_on
FROM totals
ORDER BY pct_length_trim_agent_name_5 DESC, last_agent_hired_on NULLS LAST
