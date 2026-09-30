WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        tags,
        ABS(requester_id - 12) AS requester_id_distance,
        CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-03-05'
      AND opened_at < TIMESTAMP '2025-02-12 11:00:00'
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.priority_group,
        su.hired_on AS agent_hired_on
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (ORDER BY agent_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        SIZE(COLLECT_SET(priority_group) OVER (PARTITION BY agent_id, YEAR(agent_hired_on))) AS distinct_priority_group_in_agent_id_agent_hired_,
        NTILE(10) OVER (PARTITION BY agent_id, priority_group ORDER BY closed_at DESC NULLS FIRST, id) AS closed_at_decile,
        SUM(requester_id) OVER (PARTITION BY agent_id, YEAR(agent_hired_on)) AS agent_id_agent_hired_on_year_sum_requester_id,
        ROW_NUMBER() OVER w_all_by_agent_hired_on AS agent_hired_on_seq,
        ROW_NUMBER() OVER (PARTITION BY agent_id, priority_group ORDER BY closed_at DESC NULLS FIRST, id) AS seq_no,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_id ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        RANK() OVER (PARTITION BY agent_id ORDER BY agent_hired_on DESC) AS agent_hired_on_rank,
        MIN(requester_id) OVER (ORDER BY agent_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY agent_id, priority_group ORDER BY closed_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_closed_at,
        LAST_VALUE(agent_hired_on) OVER (PARTITION BY agent_id, priority_group ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_closed_at_to_date_priority_g, 0), 4) AS requester_id_share_of_closed_at_to_date_priority
    FROM joined_agent
    WINDOW w_all_by_agent_hired_on AS (ORDER BY agent_hired_on, id), w_agent_id_agent_hired_on_year AS (PARTITION BY agent_id, YEAR(agent_hired_on)), w_closed_at_to_date_priority_g AS (PARTITION BY TO_DATE(closed_at), priority_group)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(second_requester_id) OVER (PARTITION BY priority_group, agent_hired_on_rank ORDER BY first_known_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_second_requester_id_so_far,
        SUM(first_known_requester_id) OVER (PARTITION BY TO_DATE(closed_at), distinct_priority_group_in_agent_id_agent_hired_) AS closed_at_to_date_distinct_priority_group_in_age,
        NTH_VALUE(min_requester_id_so_far, 2) OVER (PARTITION BY priority_group, agent_hired_on_rank ORDER BY first_known_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_requester_id_so_far
    FROM ranked
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_priority_group_in_agent_id_agent_hired_,
        agent_hired_on_seq,
        closed_at_decile,
        SUM(min_requester_id_so_far) AS total_min_requester_id_so_far,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(seq_no) AS total_seq_no,
        MIN(agent_hired_on) AS first_agent_hired_on,
        ROUND(STDDEV(agent_id_agent_hired_on_year_sum_requester_id), 2) AS stddev_agent_id_agent_hired_on_year_sum_requeste,
        MAX(requester_id) AS max_requester_id
    FROM windowed
    GROUP BY 1, 2, 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(max_requester_id) OVER (PARTITION BY distinct_ids, YEAR(first_agent_hired_on)) AS distinct_ids_first_agent_hired_on_year_max_max_r,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_priority_group_in_agent_id_agent_hired_, closed_at_decile ORDER BY stddev_agent_id_agent_hired_on_year_sum_requeste), 3) AS stddev_agent_id_agent_hired_on_year_sum_requeste_2,
        LAST_VALUE(first_agent_hired_on) OVER (PARTITION BY distinct_priority_group_in_agent_id_agent_hired_, closed_at_decile ORDER BY stddev_agent_id_agent_hired_on_year_sum_requeste, distinct_priority_group_in_agent_id_agent_hired_, agent_hired_on_seq, closed_at_decile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_agent_hired_on
    FROM per_group
)
SELECT
    distinct_priority_group_in_agent_id_agent_hired_,
    agent_hired_on_seq,
    closed_at_decile,
    total_seq_no,
    first_agent_hired_on,
    stddev_agent_id_agent_hired_on_year_sum_requeste,
    max_requester_id,
    distinct_ids_first_agent_hired_on_year_max_max_r,
    stddev_agent_id_agent_hired_on_year_sum_requeste_2,
    COALESCE(agent_hired_on_seq, 0) AS agent_hired_on_seq_filled
FROM metrics
