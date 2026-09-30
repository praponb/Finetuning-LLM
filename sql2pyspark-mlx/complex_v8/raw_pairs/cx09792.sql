WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE actor <> 'customer'
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_clean,
        s.event_type_ref,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id <= 9
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_ticket_csat, 3) AS ticket_csat_percent_rank,
        LAG(ticket_closed_at) OVER (ORDER BY ticket_csat DESC, id) AS prev_ticket_closed_at
    FROM joined
    WINDOW w_all_by_ticket_csat AS (ORDER BY ticket_csat DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_ref), ' ', '_')) AS event_type_ref_slug,
        INITCAP(TRIM(event_type_ref)) AS event_type_ref_clean,
        ticket_csat + ticket_csat_percent_rank AS ticket_csat_plus_ticket_csat_percent_rank
    FROM ranked
),
ticket_agent_id_stats AS (
    -- Avg ticket_csat_plus_ticket_csat_percent_rank per ticket_agent_id
    SELECT ticket_agent_id, AVG(ticket_csat_plus_ticket_csat_percent_rank) AS avg_ticket_csat_plus_ticket_csat_percent_rank
    FROM annotated
    GROUP BY ticket_agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ticket_csat_plus_ticket_csat_percent_rank,
        ROUND(s.ticket_csat_plus_ticket_csat_percent_rank - g.avg_ticket_csat_plus_ticket_csat_percent_rank, 2) AS ticket_csat_plus_ticket_csat_percent_rank_minus_
    FROM annotated s
    JOIN ticket_agent_id_stats g ON g.ticket_agent_id = s.ticket_agent_id
    WHERE s.ticket_csat_plus_ticket_csat_percent_rank > g.avg_ticket_csat_plus_ticket_csat_percent_rank
),
with_children AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_ref,
        s.ticket_csat,
        s.ticket_agent_id,
        s.ticket_closed_at,
        s.ticket_csat_percent_rank,
        s.prev_ticket_closed_at,
        s.event_type_ref_slug,
        s.event_type_ref_clean,
        s.avg_ticket_csat_plus_ticket_csat_percent_rank,
        s.ticket_csat_plus_ticket_csat_percent_rank_minus_,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at_2,
        t.tags AS ticket_tags
    FROM vs_group s
    LEFT JOIN tickets t ON t.agent_id = s.ticket_agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type_ref,
        ticket_csat,
        ticket_agent_id,
        event_type_ref_slug,
        event_type_ref_clean,
        avg_ticket_csat_plus_ticket_csat_percent_rank,
        ticket_id_2,
        ticket_closed_at_2,
        ticket_tags,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at))) AS distinct_ticket_id_in_ticket_closed_at_month,
        NTILE(5) OVER (PARTITION BY ticket_agent_id, ticket_csat ORDER BY avg_ticket_csat_plus_ticket_csat_percent_rank, id, ticket_id_2) AS avg_ticket_csat_plus_ticket_csat_percent_rank_qu,
        RANK() OVER (PARTITION BY ticket_agent_id, ticket_csat ORDER BY avg_ticket_csat_plus_ticket_csat_percent_rank, id, ticket_id_2) AS avg_ticket_csat_plus_ticket_csat_percent_rank_ra,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_agent_id, ticket_csat ORDER BY avg_ticket_csat_plus_ticket_csat_percent_rank), 3) AS avg_ticket_csat_plus_ticket_csat_percent_rank_pe,
        LAG(event_at, 3) OVER (PARTITION BY ticket_agent_id, ticket_csat ORDER BY avg_ticket_csat_plus_ticket_csat_percent_rank, id, ticket_id_2) AS prev3_event_at
    FROM with_children
)
-- Final projection
SELECT
    id,
    ticket_id,
    ticket_csat,
    ticket_agent_id,
    event_type_ref_clean,
    avg_ticket_csat_plus_ticket_csat_percent_rank,
    ticket_closed_at_2,
    ticket_tags,
    distinct_ticket_id_in_ticket_closed_at_month,
    avg_ticket_csat_plus_ticket_csat_percent_rank_qu,
    avg_ticket_csat_plus_ticket_csat_percent_rank_pe,
    LOWER(REGEXP_REPLACE(TRIM(event_type_ref), ' ', '_')) AS event_type_ref_slug_2,
    CONCAT(event_type_ref_clean, '#', CAST(ticket_id_2 AS STRING)) AS event_type_ref_clean_ref
FROM with_trends
ORDER BY id NULLS LAST
