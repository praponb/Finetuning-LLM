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
        CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group,
        HOUR(closed_at) AS closed_at_hour,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        priority_group,
        csat_filled,
        LEAD(opened_at) OVER (PARTITION BY csat_filled, csat ORDER BY csat_filled DESC, id) AS next_opened_at,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY csat ASC) AS csat_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY csat_filled, csat ORDER BY csat_filled DESC), 3) AS csat_filled_cume_dist,
        COUNT(*) OVER (PARTITION BY TO_DATE(opened_at)) AS rows_in_opened_at_to_date,
        csat - LAG(csat) OVER w_channel_agent_id_by_csat AS csat_change,
        LAG(closed_at_hour, 3) OVER (PARTITION BY channel, agent_id ORDER BY csat ASC, id) AS prev3_closed_at_hour
    FROM filtered_tickets
    WINDOW w_channel_agent_id_by_csat AS (PARTITION BY channel, agent_id ORDER BY csat ASC, id)
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM qualified s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
channel_stats AS (
    -- Max csat_filled per channel
    SELECT channel, MAX(csat_filled) AS max_csat_filled
    FROM summarized_rows
    GROUP BY channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_csat_filled,
        ROUND(s.csat_filled - g.max_csat_filled, 2) AS csat_filled_minus_max
    FROM summarized_rows s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat_filled = g.max_csat_filled
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        INITCAP(TRIM(priority_group)) AS priority_group_clean,
        ABS(prev3_closed_at_hour - 10) AS prev3_closed_at_hour_distance
    FROM compared
)
SELECT *
FROM (
    SELECT closed_at, event_count, channel, csat
    FROM derived
) src
PIVOT (
    MAX(csat)
    FOR channel IN ('web', 'email', 'chat', 'phone')
)
ORDER BY closed_at, event_count
