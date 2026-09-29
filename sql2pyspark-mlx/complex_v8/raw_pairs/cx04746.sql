WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
      AND event_type IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        DENSE_RANK() OVER w_actor_by_event_at_ties AS event_at_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        RANK() OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_rank,
        LAG(event_at) OVER w_actor_by_event_at_2 AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(3) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS event_at_tercile,
        NTILE(4) OVER w_all_by_event_at AS event_at_quartile,
        LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at_2,
        LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id) AS prev_event_at_3,
        LEAD(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id) AS next2_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id) AS rn
    FROM valid
    WINDOW w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at), w_actor_by_event_at_2 AS (PARTITION BY actor ORDER BY event_at DESC, id), w_all_by_event_at AS (ORDER BY event_at, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        next2_event_at + INTERVAL 1 DAYS AS next2_event_at_plus_1d,
        CONCAT(event_type, '#', CAST(rn AS STRING)) AS event_type_ref
    FROM metrics
    WHERE event_at_tercile < 8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_percent_rank,
        first_known_event_at,
        event_at_cume_dist,
        event_at_rank,
        prev_event_at,
        minutes_since_prev_event_at,
        event_at_tercile,
        event_at_quartile,
        prev_event_at_2,
        next2_event_at,
        next2_event_at_plus_1d,
        event_type_ref,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at_dense_rank DESC, id), 3) AS event_at_dense_rank_percent_rank,
        SUM(event_at_percent_rank) OVER (PARTITION BY TO_DATE(next2_event_at_plus_1d), ticket_id ORDER BY event_at_tercile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_percent_rank,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor)) AS distinct_ticket_id_in_actor
    FROM annotated
),
summary AS (
    -- Summarise per group
    SELECT
        event_at_quartile,
        ROUND(SUM(event_at_tercile) / NULLIF(SUM(event_at_percent_rank), 0), 3) AS event_at_tercile_to_event_at_percent_rank_ratio,
        SUM(CASE WHEN actor IS NOT NULL THEN event_at_quartile ELSE 0 END) AS event_at_quartile_actor_is_not_null,
        COUNT(*) AS row_count,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        SUM(event_at_quartile) AS total_event_at_quartile,
        COUNT(*) FILTER (WHERE event_type = 'escalated') AS n_event_type_escalated
    FROM with_trends
    GROUP BY 1
)
SELECT
    event_at_quartile,
    event_at_tercile_to_event_at_percent_rank_ratio,
    event_at_quartile_actor_is_not_null,
    row_count,
    distinct_event_types,
    total_event_at_quartile,
    n_event_type_escalated,
    ROUND(n_event_type_escalated / NULLIF(row_count, 0), 3) AS n_event_type_escalated_per_row_count
FROM summary
WHERE total_event_at_quartile > 13
ORDER BY event_at_quartile, event_at_quartile_actor_is_not_null
