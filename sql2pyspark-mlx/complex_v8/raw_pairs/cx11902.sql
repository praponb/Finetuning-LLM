WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 13
      AND ticket_id <= 18
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_month,
        ROW_NUMBER() OVER (ORDER BY event_at_month DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS event_at_seq,
        ROUND(PERCENT_RANK() OVER w_event_type_event_at_month_by, 3) AS event_at_month_percent_rank,
        ROUND(event_at_month / NULLIF(SUM(event_at_month) OVER w_event_at_month, 0), 4) AS event_at_month_share_of_event_at_month,
        ROUND(event_at_month - AVG(event_at_month) OVER (PARTITION BY event_at_month), 2) AS event_at_month_vs_event_at_month_avg,
        DENSE_RANK() OVER w_all_by_event_at AS event_at_dense_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM scoped_events
    WINDOW w_event_type_event_at_month_by AS (PARTITION BY event_type, event_at_month ORDER BY event_at_month DESC), w_event_at_month AS (PARTITION BY event_at_month), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(last_known_event_at) AS last_known_event_at_day,
        event_at_dense_rank,
        MIN(event_at) AS first_event_at,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(STDDEV(event_at_month_vs_event_at_month_avg), 2) AS stddev_event_at_month_vs_event_at_month_avg
    FROM with_trends
    GROUP BY TO_DATE(last_known_event_at), event_at_dense_rank
)
-- Final projection
SELECT
    last_known_event_at_day,
    event_at_dense_rank,
    first_event_at,
    actor_list,
    stddev_event_at_month_vs_event_at_month_avg
FROM totals
ORDER BY last_known_event_at_day, stddev_event_at_month_vs_event_at_month_avg
