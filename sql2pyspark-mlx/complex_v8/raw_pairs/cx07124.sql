WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type <> 'reopened'
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_csat,
        ticket_agent_id,
        ticket_opened_at,
        SUM(ticket_csat) OVER (PARTITION BY ticket_csat, actor ORDER BY ticket_opened_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_csat_sum_1_prior,
        ROUND((UNIX_TIMESTAMP(ticket_opened_at) - UNIX_TIMESTAMP(LAG(ticket_opened_at) OVER (PARTITION BY ticket_id, TO_DATE(ticket_opened_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_ticket_opened_at,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_id), 2) AS ticket_csat_vs_ticket_id_avg,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY MONTH(event_at))) AS distinct_actor_in_event_at_month,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY ticket_opened_at, id) AS seq_no,
        LAG(ticket_opened_at) OVER (PARTITION BY ticket_csat, actor ORDER BY ticket_opened_at ASC, id) AS prev_ticket_opened_at,
        ROUND(PERCENT_RANK() OVER w_ticket_csat_by_ticket_csat, 3) AS ticket_csat_percent_rank,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY MONTH(event_at)), 2) AS ticket_csat_vs_event_at_month_avg,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY MONTH(event_at)), 0), 4) AS ticket_csat_share_of_event_at_month,
        ROW_NUMBER() OVER (PARTITION BY ticket_csat, actor ORDER BY ticket_opened_at ASC, id) AS seq_no_3,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_ticket_id, 0), 4) AS ticket_csat_share_of_ticket_id,
        MAX(ticket_csat) OVER (ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ticket_csat_max_7
    FROM combined_details_ticket
    WINDOW w_ticket_csat_by_ticket_csat AS (PARTITION BY ticket_csat ORDER BY ticket_csat DESC NULLS FIRST, id), w_ticket_id AS (PARTITION BY ticket_id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        ticket_csat,
        ticket_opened_at,
        ticket_csat_sum_1_prior,
        ticket_csat_vs_ticket_id_avg,
        prev_ticket_opened_at,
        ticket_csat_vs_event_at_month_avg,
        ticket_csat_share_of_event_at_month,
        seq_no_3,
        ticket_csat_share_of_ticket_id,
        ticket_csat_max_7,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER w_ticket_id_seq_no, 0), 4) AS ticket_csat_share_of_ticket_id_seq_no,
        MIN(seq_no) OVER (PARTITION BY ticket_id, seq_no) AS ticket_id_seq_no_min_seq_no,
        DENSE_RANK() OVER w_ticket_csat_by_ticket_csat_s AS ticket_csat_share_of_ticket_id_dense_rank,
        SIZE(COLLECT_SET(ticket_agent_id) OVER (PARTITION BY ticket_id, seq_no)) AS distinct_ticket_agent_id_in_ticket_id_seq_no,
        MAX(seq_no) OVER w_ticket_id_seq_no AS ticket_id_seq_no_max_seq_no,
        ROUND(AVG(ticket_csat_max_7) OVER (PARTITION BY ticket_agent_id, distinct_actor_in_event_at_month ORDER BY ticket_csat_share_of_ticket_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_max_7_avg_3,
        FIRST_VALUE(seq_no_3) IGNORE NULLS OVER (PARTITION BY actor ORDER BY ticket_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no_3,
        LEAD(seq_no) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat_share_of_ticket_id, id) AS next_seq_no
    FROM with_windows
    WINDOW w_ticket_id_seq_no AS (PARTITION BY ticket_id, seq_no), w_ticket_csat_by_ticket_csat_s AS (PARTITION BY ticket_csat ORDER BY ticket_csat_share_of_ticket_id, id)
)
-- Final projection
SELECT
    id,
    actor,
    ticket_csat,
    ticket_csat_sum_1_prior,
    prev_ticket_opened_at,
    ticket_csat_vs_event_at_month_avg,
    seq_no_3,
    ticket_csat_share_of_ticket_id_dense_rank,
    distinct_ticket_agent_id_in_ticket_id_seq_no,
    ticket_csat_max_7_avg_3,
    first_known_seq_no_3,
    next_seq_no,
    GREATEST(ticket_csat_sum_1_prior, first_known_seq_no_3) AS greatest_ticket_csat_sum_1_prior_first_known_seq
FROM metrics
ORDER BY ticket_csat_vs_event_at_month_avg, prev_ticket_opened_at DESC
