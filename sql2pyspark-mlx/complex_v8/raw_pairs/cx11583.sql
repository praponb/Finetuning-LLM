WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN requester_id >= 18 THEN 'A' WHEN requester_id >= 5 THEN 'B' ELSE 'C' END AS requester_id_tier,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE (agent_id >= 2 OR priority = 'P1')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(requester_id) OVER (PARTITION BY channel ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        SIZE(COLLECT_SET(priority_or_unknown) OVER w_closed_at_year_agent_id) AS distinct_priority_or_unknown_in_closed_at_year_a,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LEAD(requester_id) OVER (PARTITION BY priority_or_unknown, priority ORDER BY requester_id, id) AS next_requester_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id), 0), 4) AS requester_id_share_of_closed_at_year_agent_id,
        LEAD(closed_at) OVER (PARTITION BY channel ORDER BY requester_id ASC, id) AS next_closed_at,
        ROUND(AVG(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id), 2) AS closed_at_year_agent_id_avg_requester_id,
        MIN(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id) AS closed_at_year_agent_id_min_requester_id
    FROM valid_tickets
    WINDOW w_closed_at_year_agent_id AS (PARTITION BY YEAR(closed_at), agent_id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        priority_or_unknown,
        distinct_priority_or_unknown_in_closed_at_year_a,
        second_closed_at,
        next_requester_id,
        next_closed_at,
        closed_at_year_agent_id_min_requester_id,
        SUM(next_requester_id) OVER (PARTITION BY agent_id ORDER BY next_requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_requester_id_sum_3,
        MAX(next_requester_id) OVER (PARTITION BY agent_id ORDER BY next_requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_requester_id_max_2,
        DENSE_RANK() OVER (PARTITION BY YEAR(next_closed_at), priority_or_unknown ORDER BY requester_id_share_of_closed_at_year_agent_id DESC, id) AS requester_id_share_of_closed_at_year_agent_id_de,
        COUNT(*) OVER w_next_closed_at_month_agent_i AS rows_in_next_closed_at_month_agent_id,
        NTH_VALUE(running_requester_id, 2) OVER (PARTITION BY YEAR(next_closed_at), priority_or_unknown ORDER BY requester_id_share_of_closed_at_year_agent_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_requester_id,
        LEAD(next_requester_id, 1, 0) OVER (PARTITION BY YEAR(next_closed_at), priority_or_unknown ORDER BY requester_id_share_of_closed_at_year_agent_id DESC, id) AS next_next_requester_id,
        SUM(requester_id) OVER (PARTITION BY YEAR(next_closed_at), priority_or_unknown ORDER BY requester_id_share_of_closed_at_year_agent_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_2,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY next_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id
    FROM metrics
    WINDOW w_next_closed_at_month_agent_i AS (PARTITION BY DATE_TRUNC('MONTH', next_closed_at), agent_id)
)
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    priority_or_unknown,
    next_requester_id,
    next_closed_at,
    next_requester_id_sum_3,
    next_requester_id_max_2,
    requester_id_share_of_closed_at_year_agent_id_de,
    rows_in_next_closed_at_month_agent_id,
    next_next_requester_id,
    first_known_requester_id
FROM with_windows
