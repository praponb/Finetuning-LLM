WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.csat) AS prior_csat_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) FILTER (WHERE channel NOT IN ('phone', 'web')) AS ticket_channel_count,
        COUNT(*) AS ticket_count,
        MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.last_ticket_closed_at
    FROM joined_ranges s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    opened_at,
    prior_rows_1d,
    ticket_channel_count,
    ticket_count,
    last_ticket_closed_at,
    DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label,
    CASE WHEN prior_rows_1d >= 6 THEN 'A' WHEN prior_rows_1d >= 3 THEN 'B' ELSE 'C' END AS prior_rows_1d_tier
FROM with_counts
WHERE prior_rows_1d < 6
