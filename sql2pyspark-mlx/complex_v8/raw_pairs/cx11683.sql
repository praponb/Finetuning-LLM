WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE (actor = 'agent' OR ticket_id IS NOT NULL)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        NTH_VALUE(event_at_month, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_month,
        event_at_month - LAG(event_at_month) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id) AS event_at_month_change,
        ROUND(event_at_month - AVG(event_at_month) OVER (PARTITION BY ticket_id), 2) AS event_at_month_vs_ticket_id_avg
    FROM filtered
),
event_type_stats AS (
    -- Median event_at_percent_rank per event_type
    SELECT event_type, PERCENTILE_APPROX(event_at_percent_rank, 0.5) AS median_event_at_percent_rank
    FROM windowed
    GROUP BY event_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_at_percent_rank,
        ROUND(s.event_at_percent_rank - g.median_event_at_percent_rank, 2) AS event_at_percent_rank_minus_median
    FROM windowed s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_at_percent_rank >= g.median_event_at_percent_rank
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM above_average s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor IS NOT NULL
              AND t.event_type NOT IN ('replied', 'resolved')
        )
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_month,
        s.event_at_percent_rank,
        s.event_at_month_change,
        s.event_at_month_vs_ticket_id_avg,
        s.event_at_percent_rank_minus_median,
        t.opened_at AS ticket_opened_at,
        t.priority AS ticket_priority
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id AND HOUR(t.closed_at) BETWEEN 6 AND 10
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at_month,
    event_at_percent_rank,
    event_at_month_vs_ticket_id_avg,
    event_at_percent_rank_minus_median,
    ticket_opened_at,
    DATE_TRUNC('MONTH', ticket_opened_at) AS ticket_opened_at_month
FROM with_details
WHERE event_at_month_change > -6
ORDER BY id, event_at_month NULLS LAST
