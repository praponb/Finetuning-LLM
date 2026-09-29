WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_type = 'assigned'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        RANK() OVER (PARTITION BY actor ORDER BY event_at) AS event_at_rank,
        DENSE_RANK() OVER w_actor_by_event_at_ties AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER w_event_at_year_by_event_at_ti, 3) AS event_at_cume_dist,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at) AS event_at_rank_2,
        NTILE(5) OVER w_ticket_id_by_event_at AS event_at_quintile,
        LEAD(event_at, 2) OVER (PARTITION BY event_type_or_unknown ORDER BY event_at ASC, id) AS next2_event_at,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at ASC, id), 3) AS event_at_cume_dist_2,
        RANK() OVER w_event_at_to_date_event_type AS event_at_rank_3,
        LEAD(event_at, 2) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at ASC, id) AS next2_event_at_2
    FROM prepared
    WINDOW w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at), w_event_at_year_by_event_at_ti AS (PARTITION BY YEAR(event_at) ORDER BY event_at), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at, id), w_event_at_to_date_event_type AS (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at ASC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE event_at_rank_3 <= 2
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        actor,
        event_at_rank_2,
        event_at_quintile,
        event_at_cume_dist_2,
        event_at_rank_3,
        ROUND((UNIX_TIMESTAMP(next2_event_at_2) - UNIX_TIMESTAMP(LAG(next2_event_at_2) OVER (PARTITION BY actor, event_at_dense_rank ORDER BY event_at_quintile DESC, id))) / 60, 1) AS minutes_since_prev_next2_event_at_2,
        LAST_VALUE(next2_event_at_2) OVER (PARTITION BY event_type, event_at_dense_rank ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next2_event_at_2,
        ROUND(event_at_cume_dist_2 - AVG(event_at_cume_dist_2) OVER w_event_at_quintile, 2) AS event_at_cume_dist_2_vs_event_at_quintile_avg,
        FIRST_VALUE(event_at_rank_3) OVER (PARTITION BY event_type, event_at_dense_rank ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_rank_3,
        MAX(event_at_rank_2) OVER w_event_at_quintile AS event_at_quintile_max_event_at_rank_2,
        MAX(event_at_cume_dist_2) OVER (PARTITION BY event_at_quintile) AS event_at_quintile_max_event_at_cume_dist_2,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY event_at_quintile)) AS distinct_event_type_in_event_at_quintile,
        RANK() OVER (PARTITION BY actor, event_at_dense_rank ORDER BY event_at_quintile DESC) AS event_at_quintile_rank
    FROM leaders
    WINDOW w_event_at_quintile AS (PARTITION BY event_at_quintile)
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    event_at_rank_2,
    event_at_rank_3,
    minutes_since_prev_next2_event_at_2,
    last_next2_event_at_2,
    event_at_cume_dist_2_vs_event_at_quintile_avg,
    first_event_at_rank_3,
    distinct_event_type_in_event_at_quintile,
    event_at_quintile_rank
FROM ranked
WHERE event_at_quintile < 8
ORDER BY event_at_rank_2, id
LIMIT 5
