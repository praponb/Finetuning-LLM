WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 08:00:00'
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        ticket_csat,
        ticket_closed_at,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_csat), 0), 4) AS ticket_csat_share_of_ticket_csat,
        NTILE(5) OVER (PARTITION BY YEAR(event_at), actor ORDER BY ticket_requester_id ASC, id) AS ticket_requester_id_quintile,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER w_ticket_csat, 0), 4) AS ticket_requester_id_share_of_ticket_csat,
        SIZE(COLLECT_SET(ticket_id) OVER w_ticket_csat) AS distinct_ticket_id_in_ticket_csat,
        MAX(ticket_csat) OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_csat_max_3_prior,
        LAST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_requester_id,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        MAX(ticket_requester_id) OVER (ORDER BY ticket_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_requester_id_so_far,
        FIRST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(event_at), actor ORDER BY ticket_requester_id ASC), 3) AS ticket_requester_id_cume_dist,
        COUNT(*) OVER (PARTITION BY ticket_csat) AS rows_in_ticket_csat,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY ticket_csat), 2) AS ticket_requester_id_vs_ticket_csat_avg
    FROM joined
    WINDOW w_ticket_csat AS (PARTITION BY ticket_csat)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(ticket_csat_max_3_prior, 0) AS ticket_csat_max_3_prior_filled
    FROM with_trends
)
-- Final projection
SELECT
    id,
    ticket_id,
    ticket_csat,
    ticket_csat_share_of_ticket_csat,
    ticket_requester_id_quintile,
    ticket_requester_id_share_of_ticket_csat,
    distinct_ticket_id_in_ticket_csat,
    last_known_ticket_requester_id,
    first_known_ticket_closed_at,
    max_ticket_requester_id_so_far,
    ticket_requester_id_cume_dist,
    rows_in_ticket_csat,
    ticket_requester_id_vs_ticket_csat_avg,
    ticket_csat_max_3_prior_filled
FROM derived
ORDER BY first_known_ticket_closed_at DESC
