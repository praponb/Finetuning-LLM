WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        SIZE(tags) AS tags_count,
        LENGTH(TRIM(priority)) AS priority_len,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE priority IS NOT NULL
),
agent_id_stats AS (
    -- Avg priority_len per agent_id
    SELECT agent_id, AVG(priority_len) AS avg_priority_len
    FROM valid
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_priority_len,
        ROUND(s.priority_len - g.avg_priority_len, 2) AS priority_len_minus_avg
    FROM valid s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.priority_len > g.avg_priority_len
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(tags_count) OVER (PARTITION BY channel ORDER BY priority_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_tags_count,
        LEAD(priority_len) OVER w_tags_count_by_days_since_ope AS next_priority_len
    FROM compared
    WINDOW w_tags_count_by_days_since_ope AS (PARTITION BY tags_count ORDER BY days_since_opened_at DESC, id)
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM ranked s
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.priority_len,
        s.days_since_opened_at,
        s.avg_priority_len,
        s.last_tags_count,
        s.next_priority_len,
        s.ticket_count,
        su.team AS agent_team
    FROM summarized_rows s
    JOIN support_agents su ON su.id = s.agent_id
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.tags,
        s.priority_len,
        s.days_since_opened_at,
        s.last_tags_count,
        s.next_priority_len,
        s.ticket_count,
        s.agent_team,
        su.team AS agent_team_2,
        su.agent_name
    FROM enriched s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 5
)
SELECT
    id,
    channel,
    tags,
    priority_len,
    next_priority_len,
    ticket_count,
    agent_team
FROM joined
ORDER BY channel DESC NULLS FIRST, next_priority_len
