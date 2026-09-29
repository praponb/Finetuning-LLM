WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor <> 'customer'
      AND ticket_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at) OVER w_actor_event_type_by_event_at AS prev_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at, id), 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS rn,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY event_type), 0), 4) AS days_since_event_at_share_of_event_type,
        LEAD(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS next_event_at,
        SUM(days_since_event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_event_at_sum_6_prior,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2
    FROM prepared
    WINDOW w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        minutes_since_prev_event_at,
        prev_event_at,
        last_known_event_at,
        rows_in_event_type,
        event_at_percent_rank,
        days_since_event_at_share_of_event_type,
        next_event_at,
        days_since_event_at_sum_6_prior,
        second_event_at,
        minutes_since_prev_event_at_2
    FROM ranked
    WHERE rn <= 2
      AND rows_in_event_type < 18
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    minutes_since_prev_event_at,
    prev_event_at,
    rows_in_event_type,
    event_at_percent_rank,
    days_since_event_at_share_of_event_type,
    next_event_at,
    days_since_event_at_sum_6_prior,
    minutes_since_prev_event_at_2,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
    CASE WHEN event_type IN ('assigned', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM top_rows
WHERE event_at_percent_rank <= 0.7
ORDER BY rows_in_event_type, id
LIMIT 10
