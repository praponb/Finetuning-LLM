WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE (MONTH(event_at) IN (1, 2) OR ticket_id IS NOT NULL)
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_ref,
        t.csat AS ticket_csat
    FROM recent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id, ticket_csat) AS ticket_id_ticket_csat_max_ticket_csat,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_id, ticket_csat), 0), 4) AS ticket_csat_share_of_ticket_id_ticket_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, ticket_csat ORDER BY ticket_csat DESC), 3) AS ticket_csat_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_ticket_csat_ties, 3) AS ticket_csat_cume_dist,
        FIRST_VALUE(ticket_csat) IGNORE NULLS OVER (ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_csat,
        MIN(ticket_csat) OVER w_ticket_id_ticket_csat AS ticket_id_ticket_csat_min_ticket_csat,
        COUNT(*) OVER (PARTITION BY ticket_id, ticket_csat) AS rows_in_ticket_id_ticket_csat,
        MAX(ticket_csat) OVER (ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_csat_so_far,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_ticket_id, 0), 4) AS ticket_csat_share_of_ticket_id,
        ROUND(ticket_csat - AVG(ticket_csat) OVER w_ticket_id_ticket_csat, 2) AS ticket_csat_vs_ticket_id_ticket_csat_avg
    FROM enriched_ticket
    WINDOW w_all_by_ticket_csat_ties AS (ORDER BY ticket_csat DESC), w_ticket_id_ticket_csat AS (PARTITION BY ticket_id, ticket_csat), w_actor_ticket_csat_by_ticket AS (PARTITION BY actor, ticket_csat ORDER BY ticket_csat DESC), w_ticket_id AS (PARTITION BY ticket_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(ticket_csat) IGNORE NULLS OVER (PARTITION BY ticket_csat ORDER BY ticket_csat_cume_dist NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY ticket_csat DESC, id), 3) AS ticket_csat_cume_dist_2,
        COUNT(*) OVER (PARTITION BY ticket_id, first_known_ticket_csat) AS rows_in_ticket_id_first_known_ticket_csat,
        FIRST_VALUE(ticket_csat_cume_dist) OVER (PARTITION BY ticket_id ORDER BY ticket_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_csat_cume_dist,
        MAX(ticket_csat_cume_dist) OVER (PARTITION BY ticket_id, first_known_ticket_csat) AS ticket_id_first_known_ticket_csat_max_ticket_csa,
        ROW_NUMBER() OVER w_ticket_csat_by_ticket_csat_c_2 AS rn,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_ticket_csat_tie, 3) AS ticket_csat_percent_rank_2,
        ROUND(ticket_csat_cume_dist / NULLIF(SUM(ticket_csat_cume_dist) OVER (PARTITION BY ticket_id, first_known_ticket_csat), 0), 4) AS ticket_csat_cume_dist_share_of_ticket_id_first_k
    FROM windowed
    WINDOW w_ticket_csat_by_ticket_csat_c_2 AS (PARTITION BY ticket_csat ORDER BY ticket_csat_cume_dist NULLS LAST, id), w_ticket_id_by_ticket_csat_tie AS (PARTITION BY ticket_id ORDER BY ticket_csat DESC)
)
-- Final projection
SELECT
    id,
    actor,
    actor_ref,
    ticket_csat,
    ticket_csat_share_of_ticket_id_ticket_csat,
    ticket_csat_percent_rank,
    ticket_csat_cume_dist,
    ticket_id_ticket_csat_min_ticket_csat,
    max_ticket_csat_so_far,
    ticket_csat_vs_ticket_id_ticket_csat_avg,
    ticket_csat_cume_dist_2,
    rows_in_ticket_id_first_known_ticket_csat,
    first_ticket_csat_cume_dist,
    rn,
    ticket_csat_cume_dist_share_of_ticket_id_first_k,
    LOWER(REGEXP_REPLACE(TRIM(actor_ref), ' ', '_')) AS actor_ref_slug,
    ROUND(ticket_csat_cume_dist_2 * 1.07, 2) AS ticket_csat_cume_dist_2_adj
FROM ranked
WHERE first_ticket_csat_cume_dist < 0.6
