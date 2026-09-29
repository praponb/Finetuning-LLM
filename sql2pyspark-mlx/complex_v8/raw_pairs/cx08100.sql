WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM ticket_events
    WHERE event_type IN ('replied', 'created')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_plus_2h,
        t.opened_at AS ticket_opened_at,
        su.tier AS agent_tier
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        HOUR(event_at) AS event_at_hour,
        HOUR(event_at) AS event_at_hour_2,
        DATE_TRUNC('MONTH', event_at_plus_2h) AS event_at_plus_2h_month
    FROM combined_details
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_opened_at,
        agent_tier,
        event_at_hour,
        event_at_hour_2,
        FLOOR(event_at_hour_2 / 2) * 2 AS event_at_hour_2_bucket
    FROM annotated
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM annotated_2
    WHERE event_at_hour_2_bucket <= 14
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_at_hour_2_bucket,
        PERCENTILE_APPROX(event_at_hour_2, 0.5) AS median_event_at_hour_2,
        MAX(ticket_opened_at) AS last_ticket_opened_at,
        COUNT(*) FILTER (WHERE actor = 'customer') AS n_actor_customer,
        ROUND(STDDEV(event_at_hour_2_bucket), 2) AS stddev_event_at_hour_2_bucket
    FROM enriched
    GROUP BY 1, 2
    HAVING MAX(event_at_hour_2_bucket) > 8
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(median_event_at_hour_2 / 2) * 2 AS median_event_at_hour_2_bucket
    FROM per_group
)
SELECT
    event_at_month,
    event_at_hour_2_bucket,
    last_ticket_opened_at,
    median_event_at_hour_2_bucket,
    COALESCE(event_at_hour_2_bucket, 0) AS event_at_hour_2_bucket_filled
FROM enriched_2
ORDER BY last_ticket_opened_at, event_at_month NULLS LAST
