WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND HOUR(event_at) BETWEEN 8 AND 12
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_dense_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at) AS event_at_dense_rank_2,
        NTILE(4) OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at, id) AS event_at_quartile,
        LEAD(event_at, 2) OVER w_ticket_id_event_at_year_by_e_3 AS next2_event_at,
        LAG(event_at, 3) OVER (PARTITION BY event_type, YEAR(event_at) ORDER BY event_at DESC, id) AS prev3_event_at,
        ROUND(CUME_DIST() OVER w_ticket_id_actor_by_event_at, 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at, id) AS prev_event_at
    FROM recent
    WINDOW w_ticket_id_event_at_year_by_e_3 AS (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at, id), w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE event_at_dense_rank <= 5
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER (PARTITION BY ticket_id, MONTH(last_event_at)), 0), 4) AS event_at_cume_dist_share_of_ticket_id_last_event,
        SUM(event_at_dense_rank_2) OVER w_next2_event_at_to_date AS next2_event_at_to_date_sum_event_at_dense_rank_2,
        ROUND(AVG(event_at_quartile) OVER (ORDER BY next2_event_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_at_quartile_avg_3,
        NTILE(4) OVER (PARTITION BY ticket_id ORDER BY event_at_dense_rank_2 DESC, id) AS event_at_dense_rank_2_quartile,
        COUNT(*) OVER (PARTITION BY TO_DATE(next2_event_at)) AS rows_in_next2_event_at_to_date,
        ROUND(event_at_quartile - AVG(event_at_quartile) OVER (PARTITION BY TO_DATE(next2_event_at)), 2) AS event_at_quartile_vs_next2_event_at_to_date_avg,
        SIZE(COLLECT_SET(actor) OVER w_ticket_id_last_event_at_mont) AS distinct_actor_in_ticket_id_last_event_at_month,
        COUNT(*) OVER w_ticket_id_last_event_at_mont AS rows_in_ticket_id_last_event_at_month
    FROM top_rows
    WINDOW w_next2_event_at_to_date AS (PARTITION BY TO_DATE(next2_event_at)), w_ticket_id_last_event_at_mont AS (PARTITION BY ticket_id, MONTH(last_event_at))
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    first_event_at,
    last_event_at,
    event_at_dense_rank_2,
    event_at_quartile,
    prev3_event_at,
    event_at_cume_dist,
    second_event_at,
    prev_event_at,
    next2_event_at_to_date_sum_event_at_dense_rank_2,
    event_at_quartile_avg_3,
    event_at_quartile_vs_next2_event_at_to_date_avg,
    rows_in_ticket_id_last_event_at_month,
    TO_DATE(event_at) AS event_at_date
FROM metrics
WHERE actor = 'agent'
ORDER BY event_type DESC, id
LIMIT 5
