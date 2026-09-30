WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'upgrade') OR requester_id >= 3)
),
grouped AS (
    -- Summarise per group
    SELECT
        agent_id,
        channel,
        YEAR(opened_at) AS opened_at_year,
        MAX(opened_at) AS last_opened_at,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        SUM(CASE WHEN channel = 'email' THEN requester_id ELSE 0 END) AS requester_id_channel_email
    FROM recent
    WHERE MONTH(opened_at) IN (1, 2)
    GROUP BY 1, 2, 3
    HAVING SUM(requester_id) > 8
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM grouped s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.csat < 5
              AND t.opened_at < TIMESTAMP '2025-02-12 14:00:00'
        )
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.csat), 2) FROM tickets t WHERE t.agent_id = s.agent_id) AS avg_ticket_csat,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM without_matches s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        agent_id,
        channel,
        opened_at_year,
        priority_list,
        requester_id_channel_email,
        avg_ticket_csat,
        last_ticket_opened_at,
        ticket_count,
        NTILE(5) OVER (PARTITION BY ticket_count ORDER BY ticket_count NULLS LAST, agent_id, channel, opened_at_year) AS ticket_count_quintile,
        NTH_VALUE(opened_at_year, 2) OVER (PARTITION BY ticket_count ORDER BY ticket_count NULLS LAST, agent_id, channel, opened_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_year
    FROM with_stats
)
SELECT *
FROM (
    SELECT agent_id, channel, requester_id_channel_email
    FROM with_trends
) src
PIVOT (
    MAX(requester_id_channel_email)
    FOR channel IN ('phone', 'web')
)
ORDER BY agent_id
