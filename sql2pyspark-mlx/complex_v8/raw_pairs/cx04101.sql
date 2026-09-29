WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE requester_id >= 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        SUM(requester_id) OVER w_agent_id_opened_at_month AS agent_id_opened_at_month_sum_requester_id,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at)), 2) AS requester_id_vs_closed_at_month_avg,
        MIN(csat) OVER (PARTITION BY agent_id, MONTH(opened_at)) AS agent_id_opened_at_month_min_csat,
        ROUND(PERCENT_RANK() OVER w_opened_at_month_csat_by_requ, 3) AS requester_id_percent_rank,
        ROW_NUMBER() OVER (ORDER BY requester_id DESC, id) AS seq_no,
        LAST_VALUE(opened_at) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        requester_id - LAG(requester_id) OVER (ORDER BY opened_at DESC NULLS FIRST, id) AS requester_id_change,
        MAX(csat) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, agent_id), 2) AS csat_agent_id_avg_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, MONTH(opened_at)), 2) AS agent_id_opened_at_month_avg_csat,
        DENSE_RANK() OVER (PARTITION BY csat, agent_id ORDER BY closed_at ASC) AS closed_at_dense_rank,
        RANK() OVER (ORDER BY opened_at DESC NULLS FIRST, id) AS opened_at_rank,
        LAG(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat ORDER BY requester_id, id) AS prev_csat,
        LEAD(requester_id, 3, 0) OVER (ORDER BY opened_at DESC, id) AS next3_requester_id,
        LAST_VALUE(opened_at) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at_2,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at
    FROM recent
    WINDOW w_agent_id_opened_at_month AS (PARTITION BY agent_id, MONTH(opened_at)), w_opened_at_month_csat_by_requ AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat ORDER BY requester_id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        agent_id_opened_at_month_sum_requester_id,
        requester_id_vs_closed_at_month_avg,
        agent_id_opened_at_month_min_csat,
        requester_id_percent_rank,
        seq_no,
        last_opened_at,
        requester_id_change,
        max_csat_so_far,
        csat_agent_id_avg_requester_id,
        agent_id_opened_at_month_avg_csat,
        opened_at_rank,
        prev_csat,
        next3_requester_id,
        last_opened_at_2,
        second_closed_at
    FROM with_trends
    WHERE closed_at_dense_rank <= 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        closed_at,
        priority,
        agent_id_opened_at_month_sum_requester_id,
        requester_id_vs_closed_at_month_avg,
        agent_id_opened_at_month_min_csat,
        requester_id_percent_rank,
        seq_no,
        last_opened_at,
        requester_id_change,
        csat_agent_id_avg_requester_id,
        opened_at_rank,
        prev_csat,
        next3_requester_id,
        last_opened_at_2,
        NTILE(4) OVER w_prev_csat_last_opened_at_2_t AS last_opened_at_2_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY prev_csat, TO_DATE(last_opened_at_2) ORDER BY last_opened_at_2 DESC NULLS FIRST), 3) AS last_opened_at_2_cume_dist,
        DENSE_RANK() OVER (PARTITION BY prev_csat, DATE_TRUNC('MONTH', last_opened_at) ORDER BY requester_id ASC) AS requester_id_dense_rank,
        requester_id_change - LAG(requester_id_change) OVER (PARTITION BY prev_csat, TO_DATE(last_opened_at_2) ORDER BY last_opened_at_2 DESC NULLS FIRST, id) AS requester_id_change_change,
        MAX(max_csat_so_far) OVER (PARTITION BY opened_at_rank, agent_id) AS opened_at_rank_agent_id_max_max_csat_so_far,
        COUNT(*) OVER (PARTITION BY opened_at_rank, agent_id) AS rows_in_opened_at_rank_agent_id,
        SUM(max_csat_so_far) OVER (PARTITION BY opened_at_rank, agent_id) AS opened_at_rank_agent_id_sum_max_csat_so_far,
        ROUND(next3_requester_id - AVG(next3_requester_id) OVER (PARTITION BY opened_at_rank, agent_id), 2) AS next3_requester_id_vs_opened_at_rank_agent_id_av
    FROM top_rows
    WINDOW w_prev_csat_last_opened_at_2_t AS (PARTITION BY prev_csat, TO_DATE(last_opened_at_2) ORDER BY last_opened_at_2 DESC NULLS FIRST, id)
)
SELECT
    id,
    opened_at,
    closed_at,
    priority,
    agent_id_opened_at_month_sum_requester_id,
    agent_id_opened_at_month_min_csat,
    last_opened_at,
    requester_id_change,
    csat_agent_id_avg_requester_id,
    prev_csat,
    last_opened_at_2,
    last_opened_at_2_quartile,
    last_opened_at_2_cume_dist,
    requester_id_dense_rank,
    requester_id_change_change,
    opened_at_rank_agent_id_max_max_csat_so_far,
    rows_in_opened_at_rank_agent_id,
    opened_at_rank_agent_id_sum_max_csat_so_far,
    DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM windowed
ORDER BY priority NULLS LAST, last_opened_at DESC NULLS FIRST, id
LIMIT 20
