WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        CASE WHEN event_type IN ('reopened', 'resolved') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        event_type_prefix,
        event_type_group,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(10) OVER w_actor_by_event_at AS event_at_decile,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at DESC) AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER w_actor_by_event_at, 3) AS event_at_cume_dist_2,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), 3) AS event_at_cume_dist_3,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at, id), 3) AS event_at_percent_rank,
        LAG(event_at, 3) OVER w_all_by_event_at AS prev3_event_at
    FROM base
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at DESC, id), w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at DESC), w_all_by_event_at AS (ORDER BY event_at, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        event_type_prefix,
        event_at_cume_dist,
        first_known_event_at,
        second_event_at,
        event_at_decile,
        event_at_cume_dist_2,
        event_at_cume_dist_3,
        event_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(first_known_event_at) - UNIX_TIMESTAMP(last_event_at)) / 60, 1) AS minutes_last_event_at_to_first_known_event_at,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM metrics
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type_prefix,
        event_at_cume_dist,
        event_at_decile,
        event_at_cume_dist_2,
        event_at_cume_dist_3,
        event_at_percent_rank,
        minutes_last_event_at_to_first_known_event_at,
        event_type_or_unknown,
        FIRST_VALUE(event_at) OVER (PARTITION BY MONTH(second_event_at), event_at_decile ORDER BY minutes_last_event_at_to_first_known_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAST_VALUE(event_at_cume_dist_3) OVER (PARTITION BY MONTH(second_event_at), event_at_decile ORDER BY minutes_last_event_at_to_first_known_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_cume_dist_3,
        SIZE(COLLECT_SET(event_type_or_unknown) OVER (PARTITION BY MONTH(event_at))) AS distinct_event_type_or_unknown_in_event_at_month
    FROM annotated
)
SELECT
    id,
    event_type_prefix,
    event_at_cume_dist,
    event_at_decile,
    event_at_cume_dist_2,
    event_at_cume_dist_3,
    event_at_percent_rank,
    minutes_last_event_at_to_first_known_event_at,
    event_type_or_unknown,
    first_event_at,
    distinct_event_type_or_unknown_in_event_at_month,
    TO_DATE(first_event_at) AS first_event_at_date,
    LOWER(REGEXP_REPLACE(TRIM(event_type_or_unknown), ' ', '_')) AS event_type_or_unknown_slug
FROM windowed
WHERE event_at_percent_rank < 0.8
