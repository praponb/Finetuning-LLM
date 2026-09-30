WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_at < TIMESTAMP '2025-03-02 15:00:00'
),
unique_rows AS (
    -- Keep the earliest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_or_unknown,
        event_at_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_or_unknown,
        DAYOFWEEK(event_at_month) AS event_at_month_dayofweek,
        SUBSTRING(TRIM(actor_or_unknown), 1, 2) AS actor_or_unknown_prefix,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM unique_rows
)
SELECT
    COALESCE(actor_or_unknown, 'ALL') AS actor_or_unknown,
    ticket_id,
    event_at_month_dayofweek,
    GROUPING(actor_or_unknown) AS is_total_actor_or_unknown,
    ROUND(SUM(days_since_event_at) / NULLIF(SUM(event_at_month_dayofweek), 0), 3) AS days_since_event_at_to_event_at_month_dayofweek_,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM tagged
GROUP BY GROUPING SETS ((actor_or_unknown, ticket_id, event_at_month_dayofweek), (actor_or_unknown), (ticket_id))
