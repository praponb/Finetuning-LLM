WITH filtered_tickets AS (
    -- Filter tickets
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
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        UPPER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE csat < 5
      AND channel NOT IN ('chat', 'email')
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM filtered_tickets s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(opened_at) AS last_ticket_opened_at,
        COUNT(*) FILTER (WHERE priority NOT IN ('P2', 'P4')) AS ticket_priority_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_opened_at,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count
    FROM with_activity s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags,
        priority_slug,
        priority_clean,
        last_ticket_opened_at,
        ticket_priority_count,
        SUM(csat) OVER (PARTITION BY ticket_priority_count, csat) AS ticket_priority_count_csat_sum_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY ticket_priority_count, csat), 0), 4) AS csat_share_of_ticket_priority_count_csat,
        MAX(requester_id) OVER w_ticket_priority_count_csat AS ticket_priority_count_csat_max_requester_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY channel ORDER BY csat DESC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        NTILE(4) OVER w_all_by_opened_at AS opened_at_quartile
    FROM with_counts
    WINDOW w_ticket_priority_count_csat AS (PARTITION BY ticket_priority_count, csat), w_all_by_opened_at AS (ORDER BY opened_at, id)
),
priority_stats AS (
    -- Avg opened_at_quartile per priority
    SELECT priority, AVG(opened_at_quartile) AS avg_opened_at_quartile
    FROM ranked
    GROUP BY priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_opened_at_quartile,
        ROUND(s.opened_at_quartile - g.avg_opened_at_quartile, 2) AS opened_at_quartile_minus_avg
    FROM ranked s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.opened_at_quartile > g.avg_opened_at_quartile
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        opened_at,
        priority,
        tags,
        priority_slug,
        last_ticket_opened_at,
        csat_share_of_ticket_priority_count_csat,
        ticket_priority_count_csat_max_requester_id,
        opened_at_quartile,
        avg_opened_at_quartile,
        opened_at_quartile_minus_avg,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_id, TO_DATE(last_ticket_opened_at) ORDER BY last_ticket_opened_at, id))) / 60, 1) AS minutes_since_prev_opened_at_2,
        NTILE(5) OVER (PARTITION BY agent_id, TO_DATE(last_ticket_opened_at) ORDER BY last_ticket_opened_at, id) AS last_ticket_opened_at_quintile
    FROM compared
)
SELECT
    id,
    requester_id,
    opened_at,
    priority,
    priority_slug,
    last_ticket_opened_at,
    ticket_priority_count_csat_max_requester_id,
    minutes_since_prev_opened_at_2,
    last_ticket_opened_at_quintile,
    ABS(opened_at_quartile - 6) AS opened_at_quartile_distance,
    CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref
FROM with_trends
ORDER BY priority, opened_at
