WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type IN ('created', 'escalated', 'resolved')
      AND actor IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_actor_or_unknown_event_at_mo, 3) AS event_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY event_at) AS event_at_dense_rank,
        LEAD(event_at, 2) OVER (PARTITION BY actor_or_unknown, MONTH(event_at) ORDER BY event_at, id) AS next2_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (ORDER BY event_at) AS event_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at NULLS LAST, id), 3) AS event_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at DESC), 3) AS event_at_percent_rank_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id), 3) AS event_at_cume_dist_2,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTILE(3) OVER (ORDER BY event_at NULLS LAST, id) AS event_at_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_or_unknown, MONTH(event_at) ORDER BY event_at, id), 3) AS event_at_percent_rank_3
    FROM cleaned
    WINDOW w_actor_or_unknown_event_at_mo AS (PARTITION BY actor_or_unknown, MONTH(event_at) ORDER BY event_at)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(event_type)) AS event_type_len,
        LENGTH(TRIM(event_type)) AS event_type_len_2
    FROM ranked
),
grouped AS (
    -- Summarise per group
    SELECT
        event_type_len,
        TO_DATE(last_event_at) AS last_event_at_day,
        event_type,
        ROUND(100 * COUNT(CASE WHEN actor = 'customer' THEN 1 END) / COUNT(*), 1) AS pct_actor_customer,
        COUNT(*) AS row_count,
        COUNT(DISTINCT actor_or_unknown) AS distinct_actor_or_unknowns
    FROM annotated
    WHERE event_at_percent_rank_3 BETWEEN 0.1 AND 0.8
    GROUP BY event_type_len, TO_DATE(last_event_at), event_type
)
-- Final projection
SELECT
    event_type_len,
    last_event_at_day,
    pct_actor_customer,
    distinct_actor_or_unknowns,
    DATE_FORMAT(last_event_at_day, 'yyyy-MM-dd') AS last_event_at_day_label,
    DATE_TRUNC('MONTH', last_event_at_day) AS last_event_at_day_month
FROM grouped
ORDER BY pct_actor_customer NULLS LAST
