WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
summary AS (
    -- Summarise per group
    SELECT
        YEAR(event_at) AS event_at_year,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN actor IN ('system', 'agent') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_system_agent,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE event_type = 'resolved') AS n_event_type_resolved
    FROM valid
    GROUP BY YEAR(event_at)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE event_at_year > (SELECT AVG(event_at_year) FROM summary)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN distinct_ids >= 9 THEN 'HIGH' WHEN distinct_ids >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_ids_tier
    FROM screened
)
-- Final projection
SELECT
    event_at_year,
    cnt,
    pct_actor_in_system_agent,
    distinct_ids,
    n_event_type_resolved,
    LOWER(REGEXP_REPLACE(TRIM(distinct_ids_tier), ' ', '_')) AS distinct_ids_tier_slug
FROM labeled
ORDER BY n_event_type_resolved NULLS LAST
