WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_type IN ('created', 'reopened', 'resolved')
),
unique_rows AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        event_at_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at_date DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(event_at) AS event_at_day,
        COUNT(*) FILTER (WHERE event_type = 'assigned') AS n_event_type_assigned,
        COUNT(*) AS cnt
    FROM unique_rows
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE n_event_type_assigned > (SELECT MIN(n_event_type_assigned) FROM per_group)
),
annotated AS (
    -- Derive helper columns
    SELECT *, FLOOR(cnt / 2) * 2 AS cnt_bucket
    FROM without_matches
    WHERE cnt < 8
)
-- Final projection
SELECT
    event_at_day,
    n_event_type_assigned,
    cnt,
    cnt_bucket,
    LAST_DAY(event_at_day) AS event_at_day_month_end,
    DATE_TRUNC('MONTH', event_at_day) AS event_at_day_month
FROM annotated
ORDER BY cnt NULLS LAST, n_event_type_assigned DESC NULLS FIRST
