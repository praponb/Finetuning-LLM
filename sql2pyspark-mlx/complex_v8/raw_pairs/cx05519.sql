WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'replied'
      AND ticket_id <= 4
),
combined_details AS (
    -- Attach ticket attributes
    SELECT s.*, t.priority AS ticket_priority
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY event_at DESC, id) AS event_at_decile,
        LAG(event_at) OVER w_ticket_priority_actor_by_eve AS prev_event_at,
        NTILE(10) OVER (PARTITION BY ticket_priority, ticket_id ORDER BY event_at DESC, id) AS event_at_decile_2,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at_2,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS seq_no,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_priority, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LEAD(event_at, 3) OVER (PARTITION BY ticket_priority, actor ORDER BY event_at DESC, id) AS next3_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_3,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC NULLS LAST), 3) AS event_at_cume_dist
    FROM combined_details
    WINDOW w_ticket_priority_actor_by_eve AS (PARTITION BY ticket_priority, actor ORDER BY event_at DESC, id), w_all_by_event_at_ties AS (ORDER BY event_at), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_priority,
        event_at_decile,
        prev_event_at,
        event_at_decile_2,
        event_at_dense_rank,
        prev_event_at_2,
        first_event_at,
        second_event_at,
        first_event_at_2,
        last_event_at,
        next3_event_at,
        first_event_at_3,
        event_at_cume_dist
    FROM windowed
    WHERE seq_no = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_priority,
        event_at_decile,
        event_at_dense_rank,
        prev_event_at_2,
        first_event_at,
        second_event_at,
        first_event_at_2,
        last_event_at,
        next3_event_at,
        first_event_at_3,
        event_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', next3_event_at) ORDER BY first_event_at ASC) AS first_event_at_dense_rank,
        ROUND(event_at_cume_dist - AVG(event_at_cume_dist) OVER w_second_event_at_year_prev_ev, 2) AS event_at_cume_dist_vs_second_event_at_year_prev_,
        MAX(event_at_decile) OVER (PARTITION BY DATE_TRUNC('MONTH', next3_event_at) ORDER BY first_event_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_at_decile_max_3_prior,
        SIZE(COLLECT_SET(ticket_priority) OVER (PARTITION BY YEAR(second_event_at), YEAR(prev_event_at_2))) AS distinct_ticket_priority_in_second_event_at_year
    FROM leaders
    WINDOW w_second_event_at_year_prev_ev AS (PARTITION BY YEAR(second_event_at), YEAR(prev_event_at_2))
),
rollup_base AS (
    -- Group-level totals
    SELECT
        YEAR(first_event_at_3) AS first_event_at_3_year,
        MIN(first_event_at_2) AS first_first_event_at_2,
        PERCENTILE_APPROX(event_at_dense_rank, 0.5) AS median_event_at_dense_rank
    FROM with_trends
    WHERE TO_DATE(first_event_at_3) < DATE '2025-01-31'
    GROUP BY 1
)
-- Final projection
SELECT
    first_event_at_3_year,
    first_first_event_at_2,
    median_event_at_dense_rank,
    DATE_TRUNC('HOUR', first_first_event_at_2) AS first_first_event_at_2_hour
FROM rollup_base
ORDER BY median_event_at_dense_rank DESC, first_event_at_3_year NULLS LAST
