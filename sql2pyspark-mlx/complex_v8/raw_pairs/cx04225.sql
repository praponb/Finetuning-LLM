WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_type = 'assigned'
      AND MONTH(event_at) IN (1, 2)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        event_type_or_unknown,
        COUNT(*) FILTER (WHERE event_type NOT IN ('reopened', 'created')) AS n_event_type_not_in_reopened_c,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        ROUND(100 * COUNT(CASE WHEN event_type = 'reopened' THEN 1 END) / COUNT(*), 1) AS pct_event_type_reopened,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null
    FROM filtered_events
    GROUP BY actor, event_type_or_unknown
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(distinct_event_types) OVER (PARTITION BY actor ORDER BY row_count, actor, event_type_or_unknown) AS next_distinct_event_types,
        SIZE(COLLECT_SET(event_type_or_unknown) OVER (PARTITION BY n_event_type_not_in_reopened_c)) AS distinct_event_type_or_unknown_in_n_event_type_n,
        ROUND(distinct_event_types / NULLIF(SUM(distinct_event_types) OVER (PARTITION BY n_event_type_not_in_reopened_c), 0), 4) AS distinct_event_types_share_of_n_event_type_not_i,
        SUM(n_event_type_is_not_null) OVER (ORDER BY n_event_type_is_not_null, actor, event_type_or_unknown ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS n_event_type_is_not_null_sum_3_prior
    FROM grouped
)
SELECT
    actor,
    event_type_or_unknown,
    n_event_type_not_in_reopened_c,
    distinct_event_types,
    pct_event_type_reopened,
    row_count,
    n_event_type_is_not_null,
    next_distinct_event_types,
    distinct_event_type_or_unknown_in_n_event_type_n,
    n_event_type_is_not_null_sum_3_prior,
    INITCAP(TRIM(actor)) AS actor_clean
FROM metrics
ORDER BY actor DESC, distinct_event_types DESC, event_type_or_unknown
LIMIT 20
