WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        LENGTH(TRIM(event_type)) AS event_type_len,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_type_len,
        TO_DATE(event_at_plus_30m) AS event_at_plus_30m_day,
        ROUND(STDDEV(event_type_len), 2) AS stddev_event_type_len,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT actor) AS distinct_actors,
        MAX(event_type_len) AS max_event_type_len,
        ROUND(100 * COUNT(CASE WHEN event_type NOT IN ('escalated', 'assigned') THEN 1 END) / COUNT(*), 1) AS pct_event_type_not_in_escalated,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM recent_events
    GROUP BY DATE_TRUNC('MONTH', event_at), event_type_len, TO_DATE(event_at_plus_30m)
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        event_at_month,
        event_type_len,
        event_at_plus_30m_day,
        n_rows,
        pct_event_type_not_in_escalated,
        ROUND(max_event_type_len / NULLIF(SUM(max_event_type_len) OVER (PARTITION BY distinct_actors, event_type_len), 0), 4) AS max_event_type_len_share_of_distinct_actors_even,
        LAST_VALUE(event_at_month) OVER (ORDER BY n_rows, event_at_month, event_type_len, event_at_plus_30m_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_month,
        NTH_VALUE(event_type_len, 2) OVER (ORDER BY n_rows, event_at_month, event_type_len, event_at_plus_30m_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_type_len,
        ROUND(distinct_event_types / NULLIF(SUM(distinct_event_types) OVER w_distinct_actors_event_type_l, 0), 4) AS distinct_event_types_share_of_distinct_actors_ev
    FROM rollup_base
    WINDOW w_distinct_actors_event_type_l AS (PARTITION BY distinct_actors, event_type_len)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE max_event_type_len_share_of_distinct_actors_even > (SELECT AVG(max_event_type_len_share_of_distinct_actors_even) FROM metrics)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE event_type_len > (SELECT AVG(event_type_len) FROM without_matches)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE n_rows > (SELECT MIN(n_rows) FROM with_activity)
)
-- Final projection
SELECT
    event_at_month,
    event_at_plus_30m_day,
    n_rows,
    pct_event_type_not_in_escalated,
    max_event_type_len_share_of_distinct_actors_even,
    last_event_at_month,
    second_event_type_len,
    distinct_event_types_share_of_distinct_actors_ev
FROM screened
ORDER BY last_event_at_month, distinct_event_types_share_of_distinct_actors_ev
