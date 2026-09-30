WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'customer'
      AND MONTH(event_at) IN (1, 3)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        LAG(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id) AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_3,
        DENSE_RANK() OVER (ORDER BY event_at DESC NULLS FIRST) AS event_at_dense_rank,
        LAST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LEAD(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id) AS next2_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC NULLS LAST), 3) AS event_at_cume_dist,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC), 3) AS event_at_cume_dist_2,
        RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at) AS event_at_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM scoped
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        prev_event_at,
        second_event_at,
        second_event_at_2,
        second_event_at_3,
        last_event_at,
        first_event_at,
        next2_event_at,
        event_at_cume_dist,
        last_known_event_at,
        event_at_cume_dist_2,
        event_at_rank,
        first_known_event_at
    FROM with_windows
    WHERE event_at_dense_rank <= 3
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at_cume_dist_2 DESC) AS event_at_cume_dist_2_dense_rank,
        ROUND(event_at_rank / NULLIF(SUM(event_at_rank) OVER (PARTITION BY event_type), 0), 4) AS event_at_rank_share_of_event_type,
        LAG(event_at) OVER (ORDER BY second_event_at_3, id) AS prev_event_at_2,
        ROUND(event_at_cume_dist_2 - AVG(event_at_cume_dist_2) OVER w_event_type, 2) AS event_at_cume_dist_2_vs_event_type_avg,
        MIN(event_at_cume_dist_2) OVER (PARTITION BY YEAR(last_event_at)) AS last_event_at_year_min_event_at_cume_dist_2,
        ROUND(AVG(event_at_cume_dist) OVER w_last_event_at_year, 2) AS last_event_at_year_avg_event_at_cume_dist,
        ROUND(AVG(event_at_cume_dist_2) OVER (ORDER BY next2_event_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS event_at_cume_dist_2_avg_2,
        LAG(event_at_cume_dist, 3) OVER (PARTITION BY ticket_id ORDER BY second_event_at_3, id) AS prev3_event_at_cume_dist
    FROM top_per_group
    WINDOW w_event_type AS (PARTITION BY event_type), w_last_event_at_year AS (PARTITION BY YEAR(last_event_at))
)
-- Final projection
SELECT
    id,
    ticket_id,
    second_event_at,
    second_event_at_2,
    first_event_at,
    next2_event_at,
    last_known_event_at,
    event_at_cume_dist_2,
    event_at_rank,
    event_at_cume_dist_2_dense_rank,
    event_at_rank_share_of_event_type,
    prev_event_at_2,
    last_event_at_year_avg_event_at_cume_dist,
    event_at_cume_dist_2_avg_2,
    prev3_event_at_cume_dist
FROM with_trends
ORDER BY second_event_at_2 DESC
