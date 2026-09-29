WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        TO_DATE(event_at) AS event_at_date_2,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
    FROM ticket_events
    WHERE (ticket_id >= 31 OR actor = 'system')
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(event_at) AS event_at_year,
        actor,
        DATE_TRUNC('MONTH', event_at_date) AS event_at_date_month,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(*) FILTER (WHERE event_type = 'reopened') AS n_event_type_reopened,
        SUM(CASE WHEN actor IN ('agent', 'customer') THEN 1 ELSE 0 END) AS n_actor_in_agent_customer,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt
    FROM recent
    GROUP BY YEAR(event_at), actor, DATE_TRUNC('MONTH', event_at_date)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE n_event_type_reopened > (SELECT AVG(n_event_type_reopened) FROM grouped)
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_at_year, actor, field, metric_value
FROM (
    SELECT event_at_year, actor, cnt, distinct_event_types, n_event_type_reopened, distinct_ids
    FROM screened
) src
UNPIVOT (
    metric_value FOR field IN (cnt, distinct_event_types, n_event_type_reopened, distinct_ids)
) unp
