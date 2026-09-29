WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE requester_id > (SELECT MIN(requester_id) FROM recent)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat,
        ROUND(csat - AVG(csat) OVER w_csat, 2) AS csat_vs_csat_avg,
        MIN(requester_id_filled) OVER (PARTITION BY csat, agent_id ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_filled_so_far,
        MIN(requester_id) OVER w_csat AS csat_min_requester_id
    FROM screened
    WINDOW w_csat AS (PARTITION BY csat)
)
SELECT
    csat,
    priority,
    agent_id,
    COUNT(*) FILTER (WHERE priority = 'P2') AS n_priority_p2,
    MIN(csat_share_of_csat) AS min_csat_share_of_csat,
    ROUND(AVG(min_requester_id_filled_so_far), 2) AS avg_min_requester_id_filled_so_far
FROM with_windows
GROUP BY GROUPING SETS ((csat, priority, agent_id), (csat), (priority), ())
ORDER BY csat NULLS LAST, priority NULLS LAST, agent_id NULLS LAST
