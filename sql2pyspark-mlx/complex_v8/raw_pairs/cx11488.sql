WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor = 'system'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        su.agent_name,
        su.tier AS agent_tier
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_csat), 3) AS ticket_csat_cume_dist,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY event_type, DATE_TRUNC('MONTH', event_at)), 2) AS days_since_event_at_vs_event_type_event_at_month,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY TO_DATE(ticket_closed_at) ORDER BY event_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_7,
        LEAD(ticket_csat) OVER w_ticket_id_ticket_csat_by_tic AS next_ticket_csat,
        COUNT(*) OVER (ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER w_ticket_csat_actor_by_days_si AS days_since_event_at_decile,
        MAX(ticket_csat) OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ticket_csat_max_7,
        LEAD(days_since_event_at, 1, 0) OVER w_all_by_ticket_csat AS next_days_since_event_at,
        SUM(ticket_csat) OVER w_ticket_csat_agent_tier AS ticket_csat_agent_tier_sum_ticket_csat,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY ticket_id, ticket_csat ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        ROUND(PERCENT_RANK() OVER w_ticket_closed_at_to_date_by_2, 3) AS event_at_percent_rank,
        LEAD(ticket_closed_at) OVER (ORDER BY ticket_csat, id) AS next_ticket_closed_at,
        SUM(days_since_event_at) OVER (PARTITION BY ticket_csat, agent_tier) AS ticket_csat_agent_tier_sum_days_since_event_at,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY agent_tier, MONTH(event_at))) AS distinct_actor_in_agent_tier_event_at_month,
        SUM(ticket_csat) OVER (ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_csat
    FROM with_details
    WINDOW w_ticket_id_ticket_csat_by_tic AS (PARTITION BY ticket_id, ticket_csat ORDER BY ticket_closed_at DESC, id), w_ticket_csat_actor_by_days_si AS (PARTITION BY ticket_csat, actor ORDER BY days_since_event_at NULLS LAST, id), w_all_by_ticket_csat AS (ORDER BY ticket_csat, id), w_ticket_csat_agent_tier AS (PARTITION BY ticket_csat, agent_tier), w_ticket_closed_at_to_date_by_2 AS (PARTITION BY TO_DATE(ticket_closed_at) ORDER BY event_at ASC)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(next_ticket_closed_at)) / 3600, 1) AS hours_next_ticket_closed_at_to_ticket_closed_at,
        YEAR(event_at) AS event_at_year
    FROM with_windows
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    ticket_closed_at,
    agent_name,
    ticket_csat_cume_dist,
    days_since_event_at_vs_event_type_event_at_month,
    ticket_csat_avg_7,
    next_ticket_csat,
    running_count,
    days_since_event_at_decile,
    ticket_csat_max_7,
    next_days_since_event_at,
    ticket_csat_agent_tier_sum_ticket_csat,
    first_known_ticket_closed_at,
    next_ticket_closed_at,
    ticket_csat_agent_tier_sum_days_since_event_at,
    distinct_actor_in_agent_tier_event_at_month,
    running_ticket_csat,
    event_type_slug,
    hours_next_ticket_closed_at_to_ticket_closed_at,
    event_at_year,
    LEAST(running_ticket_csat, ticket_csat_max_7) AS least_running_ticket_csat_ticket_csat_max_7,
    DATE_FORMAT(next_ticket_closed_at, 'MMM yyyy') AS next_ticket_closed_at_label
FROM scored
ORDER BY agent_name DESC, id
LIMIT 20
