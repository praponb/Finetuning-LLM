WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at_date) IGNORE NULLS OVER (PARTITION BY MONTH(event_at_date), actor ORDER BY event_at_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_date,
        FIRST_VALUE(event_at) OVER (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_at_date_month_event_ty)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(event_at_date), event_type ORDER BY event_at, id), 3) AS event_at_percent_rank,
        NTILE(4) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quartile,
        ROW_NUMBER() OVER (PARTITION BY MONTH(event_at_date), event_type ORDER BY event_at, id) AS event_at_seq,
        RANK() OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_rank,
        NTILE(10) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_decile,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        NTILE(4) OVER (PARTITION BY MONTH(event_at_date), actor ORDER BY event_at_date DESC NULLS FIRST, id) AS event_at_date_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id), 3) AS event_at_percent_rank_2
    FROM base
    WINDOW w_event_at_date_month_event_ty AS (PARTITION BY MONTH(event_at_date), event_type ORDER BY event_at, id), w_all_by_event_at_ties AS (ORDER BY event_at DESC NULLS FIRST)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN event_type IN ('assigned', 'reopened') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
    FROM with_trends
)
SELECT
    id,
    ticket_id,
    event_at_date,
    first_known_event_at_date,
    minutes_since_prev_event_at,
    event_at_quartile,
    event_at_rank,
    event_at_decile,
    event_at_cume_dist,
    event_at_date_quartile,
    event_at_percent_rank_2,
    event_type_group,
    LOWER(REGEXP_REPLACE(TRIM(event_type_prefix), ' ', '_')) AS event_type_prefix_slug
FROM labeled
ORDER BY event_type_prefix_slug DESC, event_at_date DESC NULLS FIRST
