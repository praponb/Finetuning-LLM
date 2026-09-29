WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CASE WHEN event_type IN ('escalated', 'reopened') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type_slug), ' ', '_')) AS event_type_slug_slug,
        CASE WHEN event_type_group IN ('other', 'group_a') THEN 'group_a' WHEN event_type_group = 'group_b' THEN 'group_b' ELSE 'other' END AS event_type_group_group
    FROM scoped_events
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 10 AND 15
              AND t.event_type NOT IN ('escalated', 'resolved')
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_at, 3) OVER w_ticket_id_event_type_by_even AS next3_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM with_activity
    WINDOW w_ticket_id_event_type_by_even AS (PARTITION BY ticket_id, event_type ORDER BY event_at, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_group_group,
        event_at_cume_dist,
        ROUND(AVG(event_at_cume_dist) OVER (PARTITION BY ticket_id ORDER BY last_known_event_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_at_cume_dist_avg_7,
        LAG(event_at, 3) OVER (PARTITION BY ticket_id ORDER BY last_known_event_at DESC, id) AS prev3_event_at,
        FIRST_VALUE(last_known_event_at) OVER (PARTITION BY ticket_id ORDER BY last_known_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_known_event_at,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        SUM(event_at_cume_dist) OVER w_actor AS actor_sum_event_at_cume_dist,
        MAX(event_at_cume_dist) OVER w_actor AS actor_max_event_at_cume_dist
    FROM with_trends
    WINDOW w_actor AS (PARTITION BY actor)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(event_type_group_group, 'UNKNOWN') AS event_type_group_group_or_unknown,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        LEAST(event_at_cume_dist, rows_in_actor) AS least_event_at_cume_dist_rows_in_actor
    FROM ranked
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_group_group,
        s.event_at_cume_dist_avg_7,
        s.prev3_event_at,
        s.first_last_known_event_at,
        s.rows_in_actor,
        s.actor_sum_event_at_cume_dist,
        s.actor_max_event_at_cume_dist,
        s.event_type_group_group_or_unknown,
        s.event_type_ref,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_type_group_group,
    prev3_event_at,
    first_last_known_event_at,
    rows_in_actor,
    actor_sum_event_at_cume_dist,
    actor_max_event_at_cume_dist,
    event_type_group_group_or_unknown,
    event_type_ref
FROM joined_ticket
ORDER BY event_type NULLS LAST, id
LIMIT 5
