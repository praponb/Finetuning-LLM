WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_date,
        t.closed_at AS ticket_closed_at
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_event_type_by_ticket_closed, 3) AS ticket_closed_at_cume_dist,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at_date) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_date,
        LAST_VALUE(event_at_date) OVER (PARTITION BY ticket_id ORDER BY event_at_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_date,
        LAST_VALUE(event_at) OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2,
        NTH_VALUE(event_at_date, 2) OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_date,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_date DESC), 3) AS event_at_date_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_date DESC, id), 3) AS event_at_date_cume_dist_2,
        RANK() OVER w_all_by_event_at_date_ties AS event_at_date_rank
    FROM joined
    WINDOW w_event_type_by_ticket_closed AS (PARTITION BY event_type ORDER BY ticket_closed_at), w_all_by_event_at_date_ties AS (ORDER BY event_at_date DESC)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', second_event_at_date) AS second_event_at_date_month,
        SUM(event_at_date_cume_dist_2) FILTER (WHERE event_type = 'escalated') AS event_at_date_cume_dist_2_event_type_escalated,
        ROUND(100 * COUNT(CASE WHEN event_type <> 'replied' THEN 1 END) / COUNT(*), 1) AS pct_event_type_replied,
        ROUND(STDDEV(event_at_date_cume_dist_2), 2) AS stddev_event_at_date_cume_dist_2,
        COUNT(*) FILTER (WHERE event_type IN ('escalated', 'created', 'assigned')) AS n_event_type_in_escalated_crea
    FROM metrics
    GROUP BY 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        second_event_at_date_month,
        stddev_event_at_date_cume_dist_2,
        n_event_type_in_escalated_crea,
        NTILE(4) OVER (PARTITION BY n_event_type_in_escalated_crea ORDER BY pct_event_type_replied, second_event_at_date_month) AS pct_event_type_replied_quartile,
        SUM(pct_event_type_replied) OVER (PARTITION BY n_event_type_in_escalated_crea ORDER BY pct_event_type_replied, second_event_at_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pct_event_type_replied
    FROM stats
)
-- Final projection
SELECT
    second_event_at_date_month,
    n_event_type_in_escalated_crea,
    pct_event_type_replied_quartile,
    running_pct_event_type_replied
FROM sequenced
WHERE second_event_at_date_month >= TIMESTAMP '2025-01-09 09:00:00'
ORDER BY n_event_type_in_escalated_crea DESC
