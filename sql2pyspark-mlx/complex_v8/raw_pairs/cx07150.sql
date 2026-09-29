WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 6 AND 10
      AND actor = 'customer'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS next_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LEAD(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS next_event_at_2,
        NTILE(3) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS event_at_tercile,
        ROUND(PERCENT_RANK() OVER w_event_type_by_event_at, 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2
    FROM valid
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        event_at_tercile - event_at_dense_rank AS event_at_tercile_minus_event_at_dense_rank,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM metrics
    WHERE event_at_label IS NOT NULL
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(next_event_at_2) AS next_event_at_2_day,
        ticket_id,
        event_at_tercile,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM derived
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    next_event_at_2_day,
    event_at_tercile,
    cnt,
    distinct_actors,
    CASE WHEN cnt >= 9 THEN 'HIGH' WHEN cnt >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier
FROM summary
WHERE next_event_at_2_day IS NOT NULL
ORDER BY cnt DESC NULLS FIRST, event_at_tercile DESC NULLS FIRST
