WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE (ticket_id >= 20 OR actor IN ('system', 'customer'))
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at_plus_15m) AS event_at_plus_15m_month,
        SUM(CASE WHEN event_type = 'resolved' THEN 1 ELSE 0 END) AS n_event_type_resolved,
        ROUND(100 * COUNT(CASE WHEN event_type NOT IN ('assigned', 'reopened') THEN 1 END) / COUNT(*), 1) AS pct_event_type_not_in_assigned_r,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        ROUND(100 * COUNT(CASE WHEN actor = 'system' THEN 1 END) / COUNT(*), 1) AS pct_actor_system
    FROM valid_events
    GROUP BY DATE_TRUNC('MONTH', event_at_plus_15m)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE n_event_type_resolved > (SELECT AVG(n_event_type_resolved) FROM grouped)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE distinct_ids < (SELECT MAX(distinct_ids) FROM screened)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(n_event_type_resolved, 2) OVER (PARTITION BY n_event_type_resolved ORDER BY distinct_ids ASC, event_at_plus_15m_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_event_type_resolved,
        LAG(event_at_plus_15m_month, 3) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids ASC, event_at_plus_15m_month) AS prev3_event_at_plus_15m_month,
        LAST_VALUE(distinct_ids) OVER (PARTITION BY n_event_type_resolved ORDER BY distinct_ids ASC, event_at_plus_15m_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_ids,
        RANK() OVER (PARTITION BY distinct_ids ORDER BY pct_event_type_not_in_assigned_r ASC) AS pct_event_type_not_in_assigned_r_rank,
        DENSE_RANK() OVER w_distinct_ids_by_distinct_ids_2 AS distinct_ids_dense_rank,
        NTILE(3) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids ASC, event_at_plus_15m_month) AS distinct_ids_tercile
    FROM without_matches
    WINDOW w_distinct_ids_by_distinct_ids_2 AS (PARTITION BY distinct_ids ORDER BY distinct_ids ASC)
),
exploded AS (
    -- One row per event_type_list_item
    SELECT
        s.event_at_plus_15m_month,
        s.n_event_type_resolved,
        s.pct_event_type_not_in_assigned_r,
        s.actor_list,
        s.distinct_ids,
        s.pct_actor_system,
        s.second_n_event_type_resolved,
        s.prev3_event_at_plus_15m_month,
        s.last_distinct_ids,
        s.pct_event_type_not_in_assigned_r_rank,
        s.distinct_ids_dense_rank,
        s.distinct_ids_tercile,
        t.event_type_list_item
    FROM with_trends s
    LATERAL VIEW OUTER EXPLODE(s.event_type_list) t AS event_type_list_item
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(pct_actor_system * 1.5, 2) AS pct_actor_system_adj,
        CONCAT(event_type_list_item, '#', CAST(distinct_ids_tercile AS STRING)) AS event_type_list_item_ref,
        COALESCE(n_event_type_resolved, 0) AS n_event_type_resolved_filled
    FROM exploded
    WHERE MONTH(prev3_event_at_plus_15m_month) IN (1, 3)
)
SELECT
    event_at_plus_15m_month,
    n_event_type_resolved,
    pct_event_type_not_in_assigned_r,
    actor_list,
    distinct_ids,
    pct_actor_system,
    second_n_event_type_resolved,
    prev3_event_at_plus_15m_month,
    pct_event_type_not_in_assigned_r_rank,
    distinct_ids_tercile,
    event_type_list_item,
    pct_actor_system_adj,
    event_type_list_item_ref,
    n_event_type_resolved_filled,
    ABS(distinct_ids_dense_rank - 7) AS distinct_ids_dense_rank_distance
FROM enriched
ORDER BY n_event_type_resolved DESC NULLS FIRST
