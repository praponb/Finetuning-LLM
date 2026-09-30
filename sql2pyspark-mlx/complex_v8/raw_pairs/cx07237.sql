WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        LENGTH(TRIM(channel)) AS channel_len,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.requester_id_per_csat,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(csat) OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat), 2) AS csat_vs_csat_avg,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        NTILE(10) OVER (ORDER BY csat, id) AS csat_decile,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY csat, YEAR(agent_hired_on) ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        ROUND(csat / NULLIF(SUM(csat) OVER w_agent_id, 0), 4) AS csat_share_of_agent_id,
        ROUND(CUME_DIST() OVER w_agent_hired_on_month_csat_by, 3) AS agent_hired_on_cume_dist,
        MIN(csat) OVER w_agent_id AS agent_id_min_csat,
        MIN(requester_id) OVER (PARTITION BY agent_id) AS agent_id_min_requester_id,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY MONTH(agent_hired_on) ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        NTH_VALUE(csat, 2) OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        SUM(csat) OVER (PARTITION BY agent_id) AS agent_id_sum_csat,
        ROUND(PERCENT_RANK() OVER w_agent_hired_on_month_csat_by_2, 3) AS agent_hired_on_percent_rank
    FROM enriched_agent
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_agent_hired_on_month_csat_by AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), csat ORDER BY agent_hired_on DESC), w_agent_hired_on_month_csat_by_2 AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), csat ORDER BY agent_hired_on DESC, id)
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(agent_hired_on_percent_rank, 3) OVER (ORDER BY second_agent_hired_on DESC NULLS FIRST, id) AS prev3_agent_hired_on_percent_rank,
        NTH_VALUE(agent_id_sum_csat, 2) OVER (PARTITION BY csat_decile, agent_id ORDER BY first_agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_id_sum_csat,
        FIRST_VALUE(max_csat_so_far) OVER (PARTITION BY csat_decile, agent_id ORDER BY first_agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_max_csat_so_far,
        SUM(csat_share_of_agent_id) OVER (ORDER BY second_agent_hired_on DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_share_of_agent_id_sum_4,
        FIRST_VALUE(second_csat) IGNORE NULLS OVER (PARTITION BY csat_decile, agent_id ORDER BY first_agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_csat,
        MAX(csat_share_of_agent_id) OVER (PARTITION BY agent_id) AS agent_id_max_csat_share_of_agent_id,
        MIN(agent_id_max_csat) OVER w_agent_id AS agent_id_min_agent_id_max_csat
    FROM with_windows
    WINDOW w_agent_id AS (PARTITION BY agent_id)
)
SELECT
    id,
    closed_at,
    csat,
    requester_id_per_csat,
    max_csat_so_far,
    csat_vs_csat_avg,
    agent_id_max_csat,
    csat_decile,
    second_agent_hired_on,
    csat_share_of_agent_id,
    agent_hired_on_cume_dist,
    agent_id_min_csat,
    agent_id_min_requester_id,
    first_agent_hired_on,
    agent_hired_on_percent_rank,
    prev3_agent_hired_on_percent_rank,
    second_agent_id_sum_csat,
    first_max_csat_so_far,
    csat_share_of_agent_id_sum_4,
    first_known_second_csat,
    agent_id_max_csat_share_of_agent_id,
    ROUND(csat_vs_csat_avg * 1.07, 2) AS csat_vs_csat_avg_adj
FROM with_windows_2
ORDER BY csat_vs_csat_avg DESC, id DESC
