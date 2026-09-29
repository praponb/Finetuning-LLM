WITH scoped_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE tier NOT IN ('T1', 'T2')
      AND YEAR(hired_on) = 2025
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.csat), 2) FROM tickets t WHERE t.agent_id = s.id) AS avg_ticket_csat,
        (SELECT COUNT(*) FILTER (WHERE t.priority IN ('P3', 'P1')) FROM tickets t WHERE t.agent_id = s.id) AS ticket_priority_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_closed_at
    FROM scoped_agents s
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at ASC, id) AS first_ticket_closed_at_tercile,
        RANK() OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at ASC) AS first_ticket_closed_at_rank,
        NTH_VALUE(avg_ticket_csat, 2) OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_ticket_csat,
        LEAD(hired_on) OVER w_tier_or_unknown_by_first_tic AS next_hired_on,
        LEAD(first_ticket_closed_at) OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at ASC, id) AS next_first_ticket_closed_at
    FROM profiled
    WINDOW w_tier_or_unknown_by_first_tic AS (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at ASC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        tier_or_unknown,
        avg_ticket_csat,
        ticket_priority_count,
        first_ticket_closed_at,
        first_ticket_closed_at_tercile,
        first_ticket_closed_at_rank,
        second_avg_ticket_csat,
        next_first_ticket_closed_at,
        LAST_VALUE(avg_ticket_csat) IGNORE NULLS OVER (ORDER BY second_avg_ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_ticket_csat,
        RANK() OVER (ORDER BY second_avg_ticket_csat) AS second_avg_ticket_csat_rank,
        ROUND(CUME_DIST() OVER (ORDER BY second_avg_ticket_csat), 3) AS second_avg_ticket_csat_cume_dist,
        ROUND(second_avg_ticket_csat / NULLIF(SUM(second_avg_ticket_csat) OVER w_first_ticket_closed_at_rank, 0), 4) AS second_avg_ticket_csat_share_of_first_ticket_clo,
        ROUND(avg_ticket_csat - AVG(avg_ticket_csat) OVER (PARTITION BY first_ticket_closed_at_rank), 2) AS avg_ticket_csat_vs_first_ticket_closed_at_rank_a,
        DENSE_RANK() OVER (ORDER BY second_avg_ticket_csat, id) AS second_avg_ticket_csat_dense_rank
    FROM windowed
    WINDOW w_first_ticket_closed_at_rank AS (PARTITION BY first_ticket_closed_at_rank)
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(second_avg_ticket_csat_rank, 2) OVER (PARTITION BY second_avg_ticket_csat_rank, ticket_priority_count ORDER BY first_ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_avg_ticket_csat_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(first_ticket_closed_at), tier_or_unknown) AS rows_in_first_ticket_closed_at_to_date_tier_or_u,
        ROUND(AVG(ticket_priority_count) OVER (PARTITION BY second_avg_ticket_csat_rank, ticket_priority_count ORDER BY first_ticket_closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_priority_count_avg_7,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY second_avg_ticket_csat_rank, ticket_priority_count ORDER BY first_ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        LAST_VALUE(ticket_priority_count) IGNORE NULLS OVER (PARTITION BY second_avg_ticket_csat_rank, ticket_priority_count ORDER BY first_ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_priority_count
    FROM metrics
),
line_level_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.hired_on,
        s.avg_ticket_csat,
        s.first_ticket_closed_at,
        s.first_ticket_closed_at_tercile,
        s.second_avg_ticket_csat_rank,
        s.second_avg_ticket_csat_share_of_first_ticket_clo,
        s.rows_in_first_ticket_closed_at_to_date_tier_or_u,
        s.ticket_priority_count_avg_7,
        s.second_hired_on,
        t.id AS ticket_id,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM metrics_2 s
    JOIN tickets t ON t.agent_id = s.id
)
-- Final projection
SELECT
    id,
    avg_ticket_csat,
    first_ticket_closed_at,
    first_ticket_closed_at_tercile,
    second_avg_ticket_csat_rank,
    second_avg_ticket_csat_share_of_first_ticket_clo,
    rows_in_first_ticket_closed_at_to_date_tier_or_u,
    ticket_priority_count_avg_7,
    ticket_id,
    ticket_csat
FROM line_level_tickets
WHERE ticket_priority_count_avg_7 < 6.4
ORDER BY rows_in_first_ticket_closed_at_to_date_tier_or_u, second_avg_ticket_csat_rank DESC
