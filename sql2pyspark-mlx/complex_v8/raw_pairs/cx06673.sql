WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(csat) AS max_ticket_csat,
        MIN(csat) AS min_ticket_csat,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.max_ticket_csat,
        t.min_ticket_csat,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM valid_tickets s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier
    FROM with_counts s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier = 'T3'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_count,
        COUNT(*) FILTER (WHERE agent_tier IN ('T1', 'T2')) AS n_agent_tier_in_t1_t2,
        COUNT(*) AS row_count,
        MAX(opened_at) AS last_opened_at,
        SUM(ticket_count) AS total_ticket_count
    FROM combined_details
    WHERE min_ticket_csat >= 2
    GROUP BY ticket_count
)
SELECT
    ticket_count,
    n_agent_tier_in_t1_t2,
    row_count,
    last_opened_at,
    total_ticket_count
FROM summary
WHERE n_agent_tier_in_t1_t2 >= 2
ORDER BY ticket_count DESC
