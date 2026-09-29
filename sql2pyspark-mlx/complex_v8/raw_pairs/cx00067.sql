WITH valid_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 08:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY YEAR(event_at), ticket_id), 0), 4) AS ticket_requester_id_share_of_event_at_year_ticke,
        MIN(ticket_requester_id) OVER (PARTITION BY YEAR(event_at), ticket_id) AS event_at_year_ticket_id_min_ticket_requester_id,
        FIRST_VALUE(ticket_csat) IGNORE NULLS OVER (PARTITION BY ticket_csat, actor ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_csat,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY YEAR(event_at), ticket_id), 0), 4) AS ticket_csat_share_of_event_at_year_ticket_id,
        SUM(ticket_requester_id) OVER (PARTITION BY YEAR(event_at), ticket_id) AS event_at_year_ticket_id_sum_ticket_requester_id,
        ROW_NUMBER() OVER w_ticket_csat_actor_by_ticket_2 AS seq_no,
        ROUND(ticket_csat - AVG(ticket_csat) OVER w_event_at_year_ticket_id, 2) AS ticket_csat_vs_event_at_year_ticket_id_avg,
        NTILE(4) OVER (ORDER BY ticket_requester_id, id) AS ticket_requester_id_quartile,
        LAST_VALUE(ticket_requester_id) OVER (ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_requester_id,
        MAX(ticket_requester_id) OVER (ORDER BY ticket_csat ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_4,
        MAX(ticket_requester_id) OVER w_event_at_year_ticket_id AS event_at_year_ticket_id_max_ticket_requester_id,
        SUM(ticket_requester_id) OVER (PARTITION BY ticket_csat, actor ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_requester_id,
        ROUND(CUME_DIST() OVER w_ticket_csat_actor_by_ticket_3, 3) AS ticket_csat_cume_dist
    FROM joined
    WINDOW w_ticket_csat_actor_by_ticket_2 AS (PARTITION BY ticket_csat, actor ORDER BY ticket_csat ASC, id), w_event_at_year_ticket_id AS (PARTITION BY YEAR(event_at), ticket_id), w_ticket_csat_actor_by_ticket_3 AS (PARTITION BY ticket_csat, actor ORDER BY ticket_csat ASC)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        seq_no,
        first_known_ticket_csat,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(*) FILTER (WHERE actor = 'customer') AS n_actor_customer,
        COUNT(*) FILTER (WHERE actor NOT IN ('customer', 'system')) AS n_actor_not_in_customer_system,
        MAX(ticket_requester_id_share_of_event_at_year_ticke) AS max_ticket_requester_id_share_of_event_at_year_t
    FROM windowed
    GROUP BY seq_no, first_known_ticket_csat
)
SELECT
    seq_no,
    first_known_ticket_csat,
    n_actor_customer,
    max_ticket_requester_id_share_of_event_at_year_t,
    COALESCE(seq_no, 0) AS seq_no_filled,
    CASE WHEN seq_no >= 9 THEN 'large' WHEN seq_no >= 4 THEN 'medium' ELSE 'small' END AS seq_no_tier
FROM rollup_base
ORDER BY n_actor_customer DESC
