WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-12'
      AND ticket_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        LAG(event_at_quarter, 2, 0) OVER (PARTITION BY actor, event_type ORDER BY event_at_quarter, id) AS prev2_event_at_quarter,
        ROUND(AVG(event_at_quarter) OVER (PARTITION BY actor, event_type ORDER BY event_at_quarter, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_at_quarter_avg_4,
        MIN(event_at_quarter) OVER (PARTITION BY event_type) AS event_type_min_event_at_quarter,
        NTILE(3) OVER (PARTITION BY event_at_quarter ORDER BY event_at_quarter DESC, id) AS event_at_quarter_tercile,
        NTILE(4) OVER (PARTITION BY event_at_quarter ORDER BY event_at_quarter DESC, id) AS event_at_quarter_quartile,
        COUNT(*) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(event_at_quarter) OVER (PARTITION BY event_type), 2) AS event_type_avg_event_at_quarter,
        SIZE(COLLECT_SET(actor) OVER w_ticket_id_event_type) AS distinct_actor_in_ticket_id_event_type,
        ROUND(AVG(event_at_quarter) OVER (ORDER BY event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS event_at_quarter_avg_2
    FROM cleaned
    WINDOW w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(event_at_quarter_tercile, 2) OVER (PARTITION BY actor, event_at_quarter_quartile ORDER BY event_at_quarter_quartile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_quarter_tercile,
        ROUND(AVG(event_at_quarter_tercile) OVER (PARTITION BY actor, event_at_quarter_quartile ORDER BY event_at_quarter_quartile, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_at_quarter_tercile_avg_4,
        MAX(running_count) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at)) AS ticket_id_event_at_month_max_running_count,
        ROUND(AVG(prev2_event_at_quarter) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at)), 2) AS ticket_id_event_at_month_avg_prev2_event_at_quar,
        MAX(event_at_quarter_quartile) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at)) AS ticket_id_event_at_month_max_event_at_quarter_qu,
        SUM(event_at_quarter_avg_4) OVER (PARTITION BY actor, event_at_quarter_quartile ORDER BY event_at_quarter_quartile, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_at_quarter_avg_4_sum_2_prior,
        ROW_NUMBER() OVER (ORDER BY event_type_min_event_at_quarter, id) AS seq_no,
        LAST_VALUE(prev2_event_at_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at_quarter_avg_4, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev2_event_at_quarter
    FROM metrics
)
SELECT
    id,
    event_at,
    event_type,
    event_at_label,
    event_type_min_event_at_quarter,
    event_at_quarter_tercile,
    event_type_avg_event_at_quarter,
    distinct_actor_in_ticket_id_event_type,
    event_at_quarter_avg_2,
    event_at_quarter_tercile_avg_4,
    ticket_id_event_at_month_max_running_count,
    ticket_id_event_at_month_avg_prev2_event_at_quar,
    ticket_id_event_at_month_max_event_at_quarter_qu,
    event_at_quarter_avg_4_sum_2_prior,
    DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
FROM with_trends
ORDER BY event_at_quarter_avg_2
