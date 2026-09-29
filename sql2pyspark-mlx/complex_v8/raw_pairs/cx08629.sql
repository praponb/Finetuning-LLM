WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year,
        TO_DATE(event_at) AS event_at_date,
        CASE WHEN event_type IN ('created', 'replied') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor = 'agent'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_year,
        event_at_date,
        event_type_group,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAST_VALUE(event_at_date) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_date,
        NTILE(4) OVER w_all_by_event_at_year AS event_at_year_quartile
    FROM prepared
    WINDOW w_all_by_event_at_year AS (ORDER BY event_at_year ASC, id)
),
ticket_id_stats AS (
    -- Max event_at_year_quartile per ticket_id
    SELECT ticket_id, MAX(event_at_year_quartile) AS max_event_at_year_quartile
    FROM with_windows
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_year_quartile,
        ROUND(s.event_at_year_quartile - g.max_event_at_year_quartile, 2) AS event_at_year_quartile_minus_max
    FROM with_windows s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_at_year_quartile = g.max_event_at_year_quartile
),
unique_rows AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_year,
        event_at_date,
        event_type_group,
        first_known_event_at,
        last_event_at_date,
        event_at_year_quartile,
        max_event_at_year_quartile,
        event_at_year_quartile_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at_year_quartile DESC, id DESC) AS recency_rank
        FROM above_average
    ) ranked
    WHERE recency_rank = 1
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_at_year,
        s.event_at_date,
        s.event_type_group,
        s.first_known_event_at,
        s.event_at_year_quartile,
        s.max_event_at_year_quartile,
        s.event_at_year_quartile_minus_max,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at
    FROM unique_rows s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_at_year,
    event_type_group,
    first_known_event_at,
    max_event_at_year_quartile,
    event_at_year_quartile_minus_max,
    ticket_csat,
    ticket_closed_at
FROM with_parent
ORDER BY event_type_group NULLS LAST, id
LIMIT 20
