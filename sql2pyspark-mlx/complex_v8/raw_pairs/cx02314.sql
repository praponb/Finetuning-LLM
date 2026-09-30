WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CASE WHEN event_type IN ('reopened', 'escalated') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY ticket_id, event_type), 2) AS days_since_event_at_vs_ticket_id_event_type_avg,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id, event_type)) AS distinct_actor_in_ticket_id_event_type,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_ticket_id_event_type, 0), 4) AS days_since_event_at_share_of_ticket_id_event_typ
    FROM valid
    WINDOW w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type)
),
derived AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM with_windows
    WHERE days_since_event_at_vs_ticket_id_event_type_avg >= -140.0
)
SELECT
    distinct_actor_in_ticket_id_event_type,
    ticket_id,
    actor_group,
    GROUPING(distinct_actor_in_ticket_id_event_type) AS is_total_distinct_actor_in_ticket_id_event_type,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list
FROM derived
GROUP BY ROLLUP(distinct_actor_in_ticket_id_event_type, ticket_id, actor_group)
ORDER BY distinct_actor_in_ticket_id_event_type NULLS LAST, ticket_id NULLS LAST, actor_group NULLS LAST
