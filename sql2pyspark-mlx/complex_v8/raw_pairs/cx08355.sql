WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-02'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_type_clean,
        LEAD(event_at, 2) OVER (ORDER BY event_at ASC, id) AS next2_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY event_at ASC, id) AS event_at_dense_rank,
        NTILE(3) OVER (ORDER BY event_at ASC, id) AS event_at_tercile,
        LEAD(event_at, 2) OVER w_actor_by_event_at AS next2_event_at_2,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC, id), 3) AS event_at_cume_dist_2,
        NTILE(5) OVER (ORDER BY event_at ASC, id) AS event_at_quintile,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS next_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at ASC NULLS LAST), 3) AS event_at_cume_dist_3
    FROM scoped_events
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at ASC), w_all_by_event_at AS (ORDER BY event_at ASC, id), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at ASC NULLS LAST, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type_clean,
        next2_event_at,
        event_at_cume_dist,
        event_at_cume_dist_2,
        CONCAT_WS(': ', event_type, event_type_clean) AS event_type_event_type_clean_label,
        DATE_TRUNC('MONTH', next2_event_at) AS next2_event_at_month
    FROM ranked
    WHERE event_at_quintile BETWEEN 4 AND 9
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_event_at_cume_dist AS rn,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_event_at), TO_DATE(event_at)), 0), 4) AS event_at_cume_dist_share_of_next2_event_at_month,
        ROW_NUMBER() OVER w_all_by_event_at_cume_dist_2 AS event_at_cume_dist_seq,
        ROUND(event_at_cume_dist_2 - AVG(event_at_cume_dist_2) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_event_at), TO_DATE(event_at)), 2) AS event_at_cume_dist_2_vs_next2_event_at_month_eve,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_event_at), TO_DATE(event_at))) AS distinct_ticket_id_in_next2_event_at_month_event,
        LAG(event_at_cume_dist, 3) OVER (ORDER BY event_at_cume_dist DESC NULLS FIRST, id) AS prev3_event_at_cume_dist
    FROM annotated
    WINDOW w_all_by_event_at_cume_dist AS (ORDER BY event_at_cume_dist, id), w_all_by_event_at_cume_dist_2 AS (ORDER BY event_at_cume_dist DESC NULLS FIRST, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type_clean,
    next2_event_at,
    event_at_cume_dist_2,
    next2_event_at_month,
    rn,
    event_at_cume_dist_share_of_next2_event_at_month,
    event_at_cume_dist_2_vs_next2_event_at_month_eve,
    distinct_ticket_id_in_next2_event_at_month_event,
    DATE_TRUNC('WEEK', event_at) AS event_at_week
FROM with_windows
ORDER BY event_at_week, id
LIMIT 5
