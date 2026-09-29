WITH prepared AS (
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
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE SIZE(tags) >= 1
      AND channel IS NOT NULL
),
deduped AS (
    -- Keep the earliest row per channel
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
        least_csat_requester_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY requester_id, id) AS recency_rank
        FROM prepared
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        RANK() OVER w_all_by_opened_at_ties AS opened_at_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY TO_DATE(opened_at), priority), 2) AS csat_vs_opened_at_to_date_priority_avg
    FROM deduped
    WINDOW w_all_by_opened_at_ties AS (ORDER BY opened_at DESC)
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.tags,
        s.opened_at_rank,
        s.csat_vs_opened_at_to_date_priority_avg,
        su.hired_on AS agent_hired_on
    FROM windowed s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier NOT IN ('T3', 'T2')
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM enriched_agent s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
agent_id_stats AS (
    -- Median csat_vs_opened_at_to_date_priority_avg per agent_id
    SELECT agent_id, PERCENTILE_APPROX(csat_vs_opened_at_to_date_priority_avg, 0.5) AS median_csat_vs_opened_at_to_date_priority_avg
    FROM with_counts
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_csat_vs_opened_at_to_date_priority_avg,
        ROUND(s.csat_vs_opened_at_to_date_priority_avg - g.median_csat_vs_opened_at_to_date_priority_avg, 2) AS csat_vs_opened_at_to_date_priority_avg_minus_med
    FROM with_counts s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat_vs_opened_at_to_date_priority_avg >= g.median_csat_vs_opened_at_to_date_priority_avg
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.tags,
        s.opened_at_rank,
        s.agent_hired_on,
        s.ticket_count,
        s.median_csat_vs_opened_at_to_date_priority_avg,
        su.hired_on AS agent_hired_on_2,
        su.tier AS agent_tier
    FROM compared s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at_rank,
    agent_hired_on,
    ticket_count,
    agent_hired_on_2,
    agent_tier
FROM with_details_agent
ORDER BY agent_tier DESC, ticket_count
