WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year,
        ABS(requester_id - 9) AS requester_id_distance,
        DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter
    FROM tickets
    WHERE agent_id <= 9
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(closed_at) AS last_ticket_closed_at,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM filtered_tickets s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        priority,
        channel,
        opened_at_quarter,
        last_ticket_closed_at,
        ticket_count_2,
        SIZE(COLLECT_SET(agent_id) OVER w_channel_ticket_count_2) AS distinct_agent_id_in_channel_ticket_count_2,
        MAX(ticket_count_2) OVER (ORDER BY ticket_count DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ticket_count_2_max_2_prior,
        RANK() OVER (ORDER BY ticket_count DESC) AS ticket_count_rank,
        ROUND((UNIX_TIMESTAMP(opened_at_quarter) - UNIX_TIMESTAMP(LAG(opened_at_quarter) OVER (ORDER BY ticket_count DESC, id))) / 60, 1) AS minutes_since_prev_opened_at_quarter,
        MIN(ticket_count) OVER w_channel_ticket_count_2 AS channel_ticket_count_2_min_ticket_count,
        ROUND(AVG(ticket_count_2) OVER w_channel_ticket_count_2, 2) AS channel_ticket_count_2_avg_ticket_count_2
    FROM with_stats
    WINDOW w_channel_ticket_count_2 AS (PARTITION BY channel, ticket_count_2)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT opened_at_quarter, ticket_count_2, priority, channel_ticket_count_2_avg_ticket_count_2
    FROM windowed
) src
PIVOT (
    SUM(channel_ticket_count_2_avg_ticket_count_2)
    FOR priority IN ('P4', 'P3')
)
ORDER BY opened_at_quarter, ticket_count_2
