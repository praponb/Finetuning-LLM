WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (HOUR(event_at) BETWEEN 9 AND 12 OR event_type IS NOT NULL)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LEAD(event_at) OVER w_all_by_event_at AS next_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at) OVER w_actor_by_event_at AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_3,
        ROW_NUMBER() OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at, id) AS event_at_seq,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id), 3) AS event_at_percent_rank
    FROM valid
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at NULLS LAST, id), w_all_by_event_at_ties AS (ORDER BY event_at)
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', prev_event_at) AS prev_event_at_month,
        event_at_seq,
        MAX(second_event_at) AS last_second_event_at,
        ROUND(AVG(event_at_rank), 2) AS avg_event_at_rank,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        PERCENTILE_APPROX(event_at_rank, 0.5) AS median_event_at_rank
    FROM ranked
    WHERE actor <> 'agent'
    GROUP BY 1, 2
)
-- Final projection
SELECT
    prev_event_at_month,
    event_at_seq,
    last_second_event_at,
    avg_event_at_rank,
    median_event_at_rank,
    YEAR(prev_event_at_month) AS prev_event_at_month_year
FROM totals
ORDER BY median_event_at_rank, avg_event_at_rank
