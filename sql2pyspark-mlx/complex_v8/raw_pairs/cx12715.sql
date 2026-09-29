WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE actor IS NOT NULL
),
actor_stats AS (
    -- Median event_at_quarter per actor
    SELECT actor, PERCENTILE_APPROX(event_at_quarter, 0.5) AS median_event_at_quarter
    FROM cleaned_events
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_at_quarter,
        ROUND(s.event_at_quarter - g.median_event_at_quarter, 2) AS event_at_quarter_minus_median
    FROM cleaned_events s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_quarter >= g.median_event_at_quarter
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.days_since_event_at,
        s.event_at_quarter_minus_median,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel
    FROM outliers s
    JOIN tickets t ON t.id = s.ticket_id
),
grouped AS (
    -- Group-level totals
    SELECT
        ticket_csat,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        actor,
        MAX(event_at) AS last_event_at,
        MIN(ticket_csat) AS min_ticket_csat,
        COUNT(*) AS n_rows
    FROM joined
    WHERE days_since_event_at < 595
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(event_at_ym)) AS event_at_ym_len,
        ABS(min_ticket_csat - 3) AS min_ticket_csat_distance
    FROM grouped
),
tagged AS (
    -- Add calculated columns
    SELECT
        ticket_csat,
        event_at_ym,
        actor,
        last_event_at,
        min_ticket_csat,
        n_rows,
        GREATEST(n_rows, event_at_ym_len) AS greatest_n_rows_event_at_ym_len
    FROM scored
    WHERE ticket_csat < 5
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown_2
    FROM tagged
)
-- Final projection
SELECT
    ticket_csat,
    event_at_ym,
    actor,
    min_ticket_csat,
    n_rows,
    greatest_n_rows_event_at_ym_len,
    actor_prefix,
    actor_or_unknown,
    CONCAT_WS('|', actor_or_unknown_2, actor_or_unknown) AS actor_or_unknown_2_actor_or_unknown_label,
    LOWER(REGEXP_REPLACE(TRIM(event_at_ym), ' ', '_')) AS event_at_ym_slug
FROM scored_2
