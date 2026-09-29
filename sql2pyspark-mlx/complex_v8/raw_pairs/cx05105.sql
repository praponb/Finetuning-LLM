WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label
    FROM tickets
    WHERE SIZE(tags) >= 1
      AND csat >= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        opened_at_label,
        ROUND(AVG(csat) OVER (PARTITION BY channel), 2) AS channel_avg_csat,
        LEAD(requester_id) OVER (PARTITION BY YEAR(closed_at) ORDER BY csat DESC, id) AS next_requester_id,
        ROUND(CUME_DIST() OVER w_csat_by_requester_id_ties, 3) AS requester_id_cume_dist,
        FIRST_VALUE(closed_at) OVER (PARTITION BY csat ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        DENSE_RANK() OVER (PARTITION BY csat ORDER BY requester_id DESC) AS requester_id_dense_rank,
        MIN(requester_id) OVER (PARTITION BY agent_id, priority) AS agent_id_priority_min_requester_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY channel, csat ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROW_NUMBER() OVER (PARTITION BY channel, csat ORDER BY requester_id DESC, id) AS rn
    FROM prepared
    WINDOW w_csat_by_requester_id_ties AS (PARTITION BY csat ORDER BY requester_id DESC)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        next_requester_id,
        requester_id_cume_dist,
        first_closed_at,
        second_csat,
        NTH_VALUE(first_closed_at, 2) OVER (ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_closed_at,
        LAG(csat, 3) OVER (ORDER BY closed_at ASC, id) AS prev3_csat,
        MIN(second_csat) OVER (PARTITION BY MONTH(first_closed_at), csat) AS first_closed_at_month_csat_min_second_csat,
        COUNT(*) OVER (PARTITION BY MONTH(first_closed_at), csat) AS rows_in_first_closed_at_month_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at ASC), 3) AS closed_at_percent_rank,
        SUM(requester_id_cume_dist) OVER (PARTITION BY channel, rn ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_cume_dist,
        ROUND(requester_id_dense_rank / NULLIF(SUM(requester_id_dense_rank) OVER (PARTITION BY MONTH(first_closed_at), csat), 0), 4) AS requester_id_dense_rank_share_of_first_closed_at,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id
    FROM ranked
)
SELECT
    id,
    closed_at,
    priority,
    channel,
    next_requester_id,
    requester_id_cume_dist,
    first_closed_at,
    second_csat,
    second_first_closed_at,
    prev3_csat,
    rows_in_first_closed_at_month_csat,
    closed_at_percent_rank,
    rows_in_agent_id
FROM sequenced
WHERE requester_id_cume_dist BETWEEN 0.1 AND 0.8
