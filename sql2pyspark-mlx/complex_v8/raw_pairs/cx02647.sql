WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat
    FROM tickets
    WHERE agent_id >= 9
      AND channel = 'chat'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS requester_id_quartile,
        LEAD(csat) OVER (PARTITION BY agent_id ORDER BY csat DESC, id) AS next_csat,
        COUNT(*) OVER (PARTITION BY csat, agent_id) AS rows_in_csat_agent_id,
        LAST_VALUE(csat) OVER (PARTITION BY agent_id, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY csat ORDER BY requester_id DESC, id), 3) AS requester_id_cume_dist,
        SUM(requester_id) OVER (PARTITION BY csat ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROUND(csat - AVG(csat) OVER w_csat_agent_id, 2) AS csat_vs_csat_agent_id_avg,
        MIN(csat) OVER (PARTITION BY agent_id, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        COUNT(*) OVER (PARTITION BY agent_id, MONTH(opened_at)) AS rows_in_agent_id_opened_at_month,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id_2
    FROM valid
    WINDOW w_csat_agent_id AS (PARTITION BY csat, agent_id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('DAY', opened_at) AS opened_at_day,
        CASE WHEN next_csat >= 5 THEN 'HIGH' WHEN next_csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS next_csat_tier
    FROM windowed
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    rows_in_csat_agent_id,
    last_csat,
    running_requester_id,
    second_csat,
    csat_vs_csat_agent_id_avg,
    rows_in_agent_id_opened_at_month,
    first_requester_id_2,
    opened_at_day,
    next_csat_tier,
    ROUND(requester_id_cume_dist * 1.07, 2) AS requester_id_cume_dist_adj
FROM scored
ORDER BY rows_in_agent_id_opened_at_month NULLS LAST
