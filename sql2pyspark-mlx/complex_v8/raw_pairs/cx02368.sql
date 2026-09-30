WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE actor IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        event_type_actor_label,
        event_at_year,
        SUM(event_at_year) OVER w_event_type AS event_type_sum_event_at_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at_year DESC NULLS FIRST, id), 3) AS event_at_year_percent_rank,
        MAX(event_at_year) OVER (PARTITION BY actor, event_at_year ORDER BY event_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_year_max_3,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_at_year ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM cleaned
    WINDOW w_event_type AS (PARTITION BY event_type)
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', second_event_at) AS second_event_at_month,
        event_at_year,
        event_type,
        ROUND(SUM(event_at_year_percent_rank) / NULLIF(SUM(event_at_year_max_3), 0), 3) AS event_at_year_percent_rank_to_event_at_year_max_,
        MAX(event_at_year_max_3) AS max_event_at_year_max_3
    FROM ranked
    GROUP BY DATE_TRUNC('MONTH', second_event_at), event_at_year, event_type
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_at_year, second_event_at_month, event_type, event_at_year_percent_rank_to_event_at_year_max_
    FROM summary
) src
PIVOT (
    AVG(event_at_year_percent_rank_to_event_at_year_max_) AS avg_v, MAX(event_at_year_percent_rank_to_event_at_year_max_) AS max_v
    FOR event_type IN ('reopened', 'replied', 'escalated')
)
