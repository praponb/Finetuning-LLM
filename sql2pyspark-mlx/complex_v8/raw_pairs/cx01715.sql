WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE event_type IN ('resolved', 'created')
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_prefix,
        s.event_type_slug,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM cleaned s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at >= TIMESTAMP '2025-01-31 08:00:00'
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM with_children_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.closed_at >= TIMESTAMP '2025-02-04 23:00:00'
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        event_type_prefix,
        event_id,
        event_event_at,
        FIRST_VALUE(event_event_at) IGNORE NULLS OVER (ORDER BY event_event_at DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_event_at,
        LAST_VALUE(event_event_at) IGNORE NULLS OVER (ORDER BY event_event_at DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_event_at
    FROM filtered
),
rollup_base AS (
    -- Group-level totals
    SELECT
        event_id,
        DATE_TRUNC('MONTH', event_event_at) AS event_event_at_month,
        COUNT(*) FILTER (WHERE event_type = 'escalated') AS n_event_type_escalated,
        COUNT(*) FILTER (WHERE event_type IN ('reopened', 'created')) AS n_event_type_in_reopened_creat,
        MAX(last_known_event_event_at) AS last_last_known_event_event_at,
        SUM(CASE WHEN event_type <> 'resolved' THEN 1 ELSE 0 END) AS n_event_type_resolved
    FROM sequenced
    GROUP BY event_id, DATE_TRUNC('MONTH', event_event_at)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', event_event_at_month) AS days_since_event_event_at_month,
        GREATEST(n_event_type_resolved, n_event_type_escalated) AS greatest_n_event_type_resolved_n_event_type_esca,
        LEAST(n_event_type_resolved, n_event_type_escalated) AS least_n_event_type_resolved_n_event_type_escalat
    FROM rollup_base
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY n_event_type_resolved, DATE_TRUNC('MONTH', event_event_at_month)) AS rows_in_n_event_type_resolved_event_event_at_mon,
        ROUND(n_event_type_escalated / NULLIF(SUM(n_event_type_escalated) OVER (PARTITION BY n_event_type_resolved, DATE_TRUNC('MONTH', event_event_at_month)), 0), 4) AS n_event_type_escalated_share_of_n_event_type_res
    FROM enriched
)
SELECT
    event_id,
    n_event_type_escalated,
    n_event_type_in_reopened_creat,
    n_event_type_resolved,
    days_since_event_event_at_month,
    greatest_n_event_type_resolved_n_event_type_esca,
    least_n_event_type_resolved_n_event_type_escalat,
    rows_in_n_event_type_resolved_event_event_at_mon,
    n_event_type_escalated_share_of_n_event_type_res,
    DATE_TRUNC('HOUR', event_event_at_month) AS event_event_at_month_hour
FROM with_windows
WHERE rows_in_n_event_type_resolved_event_event_at_mon > 8
ORDER BY least_n_event_type_resolved_n_event_type_escalat DESC NULLS FIRST
