WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_actor_label,
        s.event_type_slug,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel,
        su.hired_on AS agent_hired_on
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type_slug,
        ticket_csat,
        ticket_channel,
        agent_hired_on,
        MAX(ticket_csat) OVER w_ticket_id AS ticket_id_max_ticket_csat,
        SUM(ticket_csat) OVER (PARTITION BY ticket_id ORDER BY ticket_csat ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_csat,
        DENSE_RANK() OVER w_ticket_id_by_ticket_csat_tie AS ticket_csat_dense_rank,
        LEAD(agent_hired_on, 3) OVER w_ticket_id_by_ticket_csat AS next3_agent_hired_on,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_csat, MONTH(agent_hired_on) ORDER BY agent_hired_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_1_prior,
        SUM(ticket_csat) OVER (PARTITION BY ticket_id ORDER BY ticket_csat ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ticket_csat_sum_2,
        ROW_NUMBER() OVER (PARTITION BY ticket_csat, MONTH(agent_hired_on) ORDER BY agent_hired_on DESC, id) AS seq_no,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY YEAR(agent_hired_on)), 2) AS ticket_csat_vs_agent_hired_on_year_avg,
        MIN(ticket_csat) OVER (PARTITION BY YEAR(agent_hired_on)) AS agent_hired_on_year_min_ticket_csat,
        ROUND(AVG(ticket_csat) OVER w_ticket_id, 2) AS ticket_id_avg_ticket_csat,
        DENSE_RANK() OVER w_ticket_channel_by_ticket_csa AS ticket_csat_dense_rank_2,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY ticket_id ORDER BY ticket_csat ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        MAX(ticket_csat) OVER w_agent_hired_on_year AS agent_hired_on_year_max_ticket_csat,
        FIRST_VALUE(ticket_csat) OVER (PARTITION BY ticket_csat, MONTH(agent_hired_on) ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_csat
    FROM with_details
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_ticket_id_by_ticket_csat_tie AS (PARTITION BY ticket_id ORDER BY ticket_csat ASC NULLS LAST), w_ticket_id_by_ticket_csat AS (PARTITION BY ticket_id ORDER BY ticket_csat ASC NULLS LAST, id), w_ticket_channel_by_ticket_csa AS (PARTITION BY ticket_channel ORDER BY ticket_csat ASC NULLS LAST), w_ticket_csat_agent_hired_on_m_2 AS (PARTITION BY ticket_csat, MONTH(agent_hired_on) ORDER BY agent_hired_on DESC, id), w_agent_hired_on_year AS (PARTITION BY YEAR(agent_hired_on))
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(running_ticket_csat / NULLIF(SUM(running_ticket_csat) OVER (PARTITION BY ticket_csat, ticket_csat_dense_rank_2), 0), 4) AS running_ticket_csat_share_of_ticket_csat_ticket_,
        ROUND(ticket_csat_avg_1_prior / NULLIF(SUM(ticket_csat_avg_1_prior) OVER (PARTITION BY ticket_csat, ticket_csat_dense_rank_2), 0), 4) AS ticket_csat_avg_1_prior_share_of_ticket_csat_tic,
        ROUND(AVG(seq_no) OVER (PARTITION BY ticket_csat, ticket_csat_dense_rank_2), 2) AS ticket_csat_ticket_csat_dense_rank_2_avg_seq_no,
        DATEDIFF(next3_agent_hired_on, LAG(next3_agent_hired_on) OVER (PARTITION BY ticket_csat, ticket_csat_dense_rank_2 ORDER BY ticket_csat DESC, id)) AS days_since_prev_next3_agent_hired_on,
        COUNT(*) OVER (PARTITION BY ticket_csat, ticket_csat_dense_rank_2) AS rows_in_ticket_csat_ticket_csat_dense_rank_2,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_csat, ticket_csat_dense_rank_2), 2) AS ticket_csat_ticket_csat_dense_rank_2_avg_ticket_,
        ROUND(AVG(ticket_csat_sum_2) OVER w_ticket_csat_ticket_csat_dens, 2) AS ticket_csat_ticket_csat_dense_rank_2_avg_ticket__2,
        ticket_csat_dense_rank - LAG(ticket_csat_dense_rank) OVER (PARTITION BY MONTH(agent_hired_on), ticket_channel ORDER BY agent_hired_on_year_max_ticket_csat, id) AS ticket_csat_dense_rank_change
    FROM windowed
    WINDOW w_ticket_csat_ticket_csat_dens AS (PARTITION BY ticket_csat, ticket_csat_dense_rank_2)
)
SELECT
    id,
    ticket_csat,
    ticket_channel,
    agent_hired_on,
    ticket_id_max_ticket_csat,
    running_ticket_csat,
    ticket_csat_avg_1_prior,
    ticket_csat_sum_2,
    seq_no,
    ticket_csat_vs_agent_hired_on_year_avg,
    agent_hired_on_year_min_ticket_csat,
    ticket_id_avg_ticket_csat,
    ticket_csat_dense_rank_2,
    first_agent_hired_on,
    first_ticket_csat,
    ticket_csat_avg_1_prior_share_of_ticket_csat_tic,
    ticket_csat_ticket_csat_dense_rank_2_avg_seq_no,
    days_since_prev_next3_agent_hired_on,
    rows_in_ticket_csat_ticket_csat_dense_rank_2,
    ticket_csat_ticket_csat_dense_rank_2_avg_ticket_,
    ticket_csat_ticket_csat_dense_rank_2_avg_ticket__2,
    ticket_csat_dense_rank_change,
    ABS(ticket_csat_sum_2 - 9) AS ticket_csat_sum_2_distance
FROM metrics
ORDER BY ticket_csat_sum_2_distance DESC NULLS FIRST, days_since_prev_next3_agent_hired_on DESC NULLS FIRST
