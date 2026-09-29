WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
totals AS (
    -- Summarise per group
    SELECT
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        SUM(CASE WHEN event_type = 'reopened' THEN 1 ELSE 0 END) AS n_event_type_reopened,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN event_type IN ('reopened', 'escalated', 'resolved') THEN 1 END) / COUNT(*), 1) AS pct_event_type_in_reopened_escal,
        COUNT(*) FILTER (WHERE actor IN ('customer', 'agent')) AS n_actor_in_customer_agent,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list
    FROM valid_events
    WHERE ticket_id IS NOT NULL
    GROUP BY actor, DATE_FORMAT(event_at, 'yyyy-MM')
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM totals)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM without_matches
    WHERE distinct_ids < (SELECT MAX(distinct_ids) FROM without_matches)
),
exploded AS (
    -- One row per actor_list_item
    SELECT
        s.actor,
        s.event_at_ym,
        s.n_event_type_reopened,
        s.distinct_ids,
        s.pct_event_type_in_reopened_escal,
        s.n_actor_in_customer_agent,
        s.distinct_ticket_ids,
        t.actor_list_item_pos,
        t.actor_list_item
    FROM with_activity s
    LATERAL VIEW POSEXPLODE(s.actor_list) t AS actor_list_item_pos, actor_list_item
)
SELECT
    actor,
    distinct_ids,
    pct_event_type_in_reopened_escal,
    n_actor_in_customer_agent,
    distinct_ticket_ids,
    actor_list_item_pos,
    actor_list_item
FROM exploded
WHERE n_event_type_reopened > 2
ORDER BY actor_list_item DESC, actor NULLS LAST
