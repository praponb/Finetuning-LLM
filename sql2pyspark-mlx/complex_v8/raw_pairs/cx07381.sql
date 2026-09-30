WITH valid_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY event_at), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at
    FROM valid_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        TO_DATE(event_at) AS event_at_date,
        CASE WHEN event_type IN ('reopened', 'resolved') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM metrics
    WHERE event_type = 'created'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        event_type,
        event_at_cume_dist,
        minutes_since_prev_event_at,
        event_at_date,
        event_type_group,
        INITCAP(TRIM(event_type_group)) AS event_type_group_clean
    FROM enriched
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        event_type_group,
        DATE_FORMAT(event_at_date, 'yyyy-MM') AS event_at_date_ym,
        MIN(event_at_cume_dist) AS min_event_at_cume_dist,
        SUM(CASE WHEN event_type = 'escalated' THEN 1 ELSE 0 END) AS n_event_type_escalated
    FROM tagged
    WHERE event_type_group IN ('group_a', 'group_b')
    GROUP BY 1, 2, 3
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(n_event_type_escalated, 3) OVER (PARTITION BY n_event_type_escalated ORDER BY n_event_type_escalated, event_type, event_type_group, event_at_date_ym) AS prev3_n_event_type_escalated,
        ROUND(AVG(n_event_type_escalated) OVER (PARTITION BY n_event_type_escalated ORDER BY n_event_type_escalated, event_type, event_type_group, event_at_date_ym ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_event_type_escalated_avg_7,
        SUM(min_event_at_cume_dist) OVER w_n_event_type_escalated_event AS n_event_type_escalated_event_type_sum_min_event_
    FROM totals
    WINDOW w_n_event_type_escalated_event AS (PARTITION BY n_event_type_escalated, event_type)
)
-- Subtotals with ROLLUP
SELECT
    prev3_n_event_type_escalated,
    event_type_group,
    n_event_type_escalated,
    GROUPING(prev3_n_event_type_escalated) AS is_total_prev3_n_event_type_escalated,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(*) AS n_rows
FROM with_trends
GROUP BY ROLLUP(prev3_n_event_type_escalated, event_type_group, n_event_type_escalated)
ORDER BY prev3_n_event_type_escalated NULLS LAST, event_type_group NULLS LAST, n_event_type_escalated NULLS LAST
