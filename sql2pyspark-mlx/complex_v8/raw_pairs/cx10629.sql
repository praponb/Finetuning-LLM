WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE (agent_id <= 2 OR closed_at < TIMESTAMP '2025-02-19 21:00:00')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority, csat), 2) AS csat_vs_priority_csat_avg,
        ROUND(csat / NULLIF(SUM(csat) OVER w_closed_at_to_date_csat, 0), 4) AS csat_share_of_closed_at_to_date_csat,
        RANK() OVER (PARTITION BY priority ORDER BY csat NULLS LAST) AS csat_rank,
        SUM(csat) OVER (PARTITION BY YEAR(closed_at), csat ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY csat NULLS LAST, id) AS csat_seq,
        COUNT(*) OVER (PARTITION BY csat, MONTH(opened_at)) AS rows_in_csat_opened_at_month,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_6_prior,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id, csat ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        LAG(closed_at, 3) OVER w_agent_id_csat_by_csat AS prev3_closed_at,
        MIN(csat) OVER (PARTITION BY priority ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far
    FROM prepared
    WINDOW w_closed_at_to_date_csat AS (PARTITION BY TO_DATE(closed_at), csat), w_agent_id_csat_by_csat AS (PARTITION BY agent_id, csat ORDER BY csat, id), w_priority_csat AS (PARTITION BY priority, csat)
)
-- Final projection
SELECT
    id,
    opened_at,
    csat,
    tags,
    csat_vs_priority_csat_avg,
    csat_rank,
    running_csat,
    csat_seq,
    rows_in_csat_opened_at_month,
    prev3_closed_at,
    min_csat_so_far
FROM windowed
ORDER BY min_csat_so_far NULLS LAST, csat_rank
