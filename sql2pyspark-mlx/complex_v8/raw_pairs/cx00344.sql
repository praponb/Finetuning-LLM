WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref,
        closed_at + INTERVAL 7 DAYS AS closed_at_plus_7d
    FROM tickets
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter,
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref_2,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM valid_tickets
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at < TIMESTAMP '2025-02-18 09:00:00'
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        opened_at,
        channel,
        csat,
        channel_ref,
        closed_at_plus_7d,
        opened_at_quarter,
        SUM(csat) OVER (PARTITION BY channel, agent_id ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_sum_3_prior,
        ROW_NUMBER() OVER w_all_by_closed_at_plus_7d AS seq_no
    FROM qualified
    WINDOW w_all_by_closed_at_plus_7d AS (ORDER BY closed_at_plus_7d ASC, id)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN actor <> 'system' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM windowed s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    id,
    opened_at,
    channel,
    csat,
    channel_ref,
    csat_sum_3_prior,
    seq_no,
    last_event_at,
    event_actor_count,
    SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix,
    ROUND((UNIX_TIMESTAMP(closed_at_plus_7d) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at_plus_7d
FROM profiled
ORDER BY csat DESC, csat_sum_3_prior, id
LIMIT 5
