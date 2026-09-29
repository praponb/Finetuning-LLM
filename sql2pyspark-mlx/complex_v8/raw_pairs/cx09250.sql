WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 7 AND 12
      AND priority = 'P1'
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND TO_DATE(t.event_at) < DATE '2025-03-08'
              AND t.event_type = 'escalated'
        )
),
channel_stats AS (
    -- Median csat per channel
    SELECT channel, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM with_activity
    GROUP BY channel
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM with_activity s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat >= g.median_csat
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.median_csat,
        s.csat_minus_median,
        su.agent_name
    FROM vs_group s
    JOIN support_agents su ON su.id = s.agent_id
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_parent_agent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'email'
        )
      AND MONTH(s.closed_at) IN (2, 3)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        agent_name,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(PERCENT_RANK() OVER w_all_by_closed_at, 3) AS closed_at_percent_rank,
        LEAD(csat_minus_median, 1, 0) OVER w_all_by_closed_at AS next_csat_minus_median,
        LAG(closed_at, 3) OVER (ORDER BY closed_at, id) AS prev3_closed_at,
        ROUND(median_csat / NULLIF(SUM(median_csat) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at)), 0), 4) AS median_csat_share_of_closed_at_month,
        MAX(requester_id) OVER (ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_max_3
    FROM screened
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at, id)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM with_trends s
)
SELECT
    id,
    agent_id,
    priority,
    agent_name,
    minutes_since_prev_closed_at,
    closed_at_percent_rank,
    next_csat_minus_median,
    prev3_closed_at,
    median_csat_share_of_closed_at_month,
    requester_id_max_3,
    first_event_at,
    DATE_FORMAT(prev3_closed_at, 'yyyy-MM-dd') AS prev3_closed_at_label
FROM summarized_rows
ORDER BY id
