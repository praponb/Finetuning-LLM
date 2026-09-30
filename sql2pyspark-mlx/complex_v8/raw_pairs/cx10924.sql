WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE priority NOT IN ('P2', 'P3')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat NULLS LAST, id) AS prev_csat,
        RANK() OVER w_agent_id_csat_by_csat_ties AS csat_rank,
        MAX(csat) OVER (PARTITION BY priority, agent_id ORDER BY opened_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_max_3_prior,
        SUM(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        RANK() OVER w_agent_id_csat_by_csat AS csat_rank_2,
        csat - LAG(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat NULLS LAST, id) AS csat_change
    FROM scoped
    WINDOW w_agent_id_csat_by_csat_ties AS (PARTITION BY agent_id, csat ORDER BY csat NULLS LAST), w_agent_id_csat_by_csat AS (PARTITION BY agent_id, csat ORDER BY csat NULLS LAST, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        prev_csat,
        csat_max_3_prior,
        running_csat,
        csat_rank_2,
        csat_change
    FROM ranked
    WHERE csat_rank <= 2
      AND channel <> 'email'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE actor = 'agent') AS event_actor_count,
        COUNT(*) FILTER (WHERE actor IN ('agent', 'customer')) AS event_actor_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_actor_count_2, 0) AS event_actor_count_2
    FROM top_per_group s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
channel_stats AS (
    -- Avg prev_csat per channel
    SELECT channel, AVG(prev_csat) AS avg_prev_csat
    FROM qualified
    GROUP BY channel
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prev_csat,
        ROUND(s.prev_csat - g.avg_prev_csat, 2) AS prev_csat_minus_avg
    FROM qualified s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.prev_csat > g.avg_prev_csat
),
unique_rows AS (
    -- Keep the latest row per priority, agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        prev_csat,
        csat_max_3_prior,
        running_csat,
        csat_rank_2,
        csat_change,
        event_count,
        event_actor_count,
        event_actor_count_2,
        avg_prev_csat,
        prev_csat_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY csat_max_3_prior DESC, id DESC) AS recency_rank
        FROM above_average
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    prev_csat,
    csat_max_3_prior,
    running_csat,
    event_count,
    event_actor_count,
    event_actor_count_2,
    prev_csat_minus_avg,
    UPPER(TRIM(priority)) AS priority_clean
FROM unique_rows
ORDER BY event_count DESC, id
