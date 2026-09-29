WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-09 04:00:00'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER w_priority_closed_at_to_date, 2) AS requester_id_vs_priority_closed_at_to_date_avg,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY closed_at, id) AS rn,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY MONTH(closed_at), DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        DENSE_RANK() OVER w_agent_id_priority_by_opened AS opened_at_dense_rank,
        NTILE(4) OVER w_opened_at_year_agent_id_by_r AS requester_id_quartile,
        NTILE(10) OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY requester_id, id) AS requester_id_decile,
        COUNT(*) OVER (PARTITION BY agent_id, priority ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY priority, TO_DATE(closed_at)), 0), 4) AS requester_id_share_of_priority_closed_at_to_date,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at
    FROM scoped
    WINDOW w_priority_closed_at_to_date AS (PARTITION BY priority, TO_DATE(closed_at)), w_agent_id_priority_by_opened AS (PARTITION BY agent_id, priority ORDER BY opened_at DESC), w_opened_at_year_agent_id_by_r AS (PARTITION BY YEAR(opened_at), agent_id ORDER BY requester_id, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        requester_id_vs_priority_closed_at_to_date_avg,
        second_requester_id,
        opened_at_dense_rank,
        requester_id_quartile,
        requester_id_decile,
        running_count,
        requester_id_share_of_priority_closed_at_to_date,
        second_closed_at
    FROM sequenced
    WHERE rn = 1
)
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    priority,
    requester_id_quartile,
    requester_id_decile,
    requester_id_share_of_priority_closed_at_to_date,
    second_closed_at,
    ABS(running_count - 7) AS running_count_distance
FROM first_rows
ORDER BY running_count_distance DESC NULLS FIRST, second_closed_at
