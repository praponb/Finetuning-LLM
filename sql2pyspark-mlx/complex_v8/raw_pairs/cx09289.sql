WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags
    FROM tickets
    WHERE channel IN ('web', 'email')
      AND csat <= 4
),
latest_per_key AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.tags,
        su.agent_name
    FROM latest_per_key s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.hired_on < DATE '2025-03-30'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        SUM(requester_id) AS total_ticket_requester_id,
        COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.total_ticket_requester_id,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM joined s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
latest AS (
    -- Keep the earliest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        agent_name,
        ticket_count,
        total_ticket_requester_id,
        ticket_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY total_ticket_requester_id, id) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
latest_per_key_2 AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        agent_name,
        ticket_count,
        total_ticket_requester_id,
        ticket_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY total_ticket_requester_id DESC, id DESC) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_count,
    ticket_count_2,
    MIN(ticket_count_2) AS min_ticket_count_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest_per_key_2
GROUP BY GROUPING SETS ((ticket_count, ticket_count_2), (ticket_count))
