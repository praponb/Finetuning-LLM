WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label
    FROM tickets
    WHERE requester_id >= 3
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), priority ORDER BY closed_at) AS closed_at_rank,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY has_refund), 2) AS requester_id_vs_has_refund_avg,
        LEAD(requester_id, 2) OVER (PARTITION BY has_refund ORDER BY requester_id ASC, id) AS next2_requester_id,
        COUNT(*) OVER w_priority_agent_id AS rows_in_priority_agent_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY has_refund)) AS distinct_priority_in_has_refund,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        requester_id - LAG(requester_id) OVER (PARTITION BY has_refund ORDER BY requester_id ASC, id) AS requester_id_change,
        FIRST_VALUE(closed_at) OVER (PARTITION BY has_refund ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        ROW_NUMBER() OVER (PARTITION BY has_refund ORDER BY requester_id ASC, id) AS seq_no,
        FIRST_VALUE(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id
    FROM valid
    WINDOW w_priority_agent_id AS (PARTITION BY priority, agent_id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(first_closed_at) AS first_closed_at_date,
        ABS(closed_at_rank - 7) AS closed_at_rank_distance,
        COALESCE(requester_id_change, 0) AS requester_id_change_filled
    FROM sequenced
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        has_refund,
        closed_at_label,
        requester_id_vs_has_refund_avg,
        next2_requester_id,
        rows_in_priority_agent_id,
        distinct_priority_in_has_refund,
        requester_id_change,
        first_closed_at,
        seq_no,
        first_requester_id,
        closed_at_rank_distance,
        requester_id_change_filled,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_share_of_agent_id,
        ROUND(requester_id_change_filled / NULLIF(SUM(requester_id_change_filled) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_change_filled_share_of_agent_id
    FROM annotated
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    tags,
    has_refund,
    closed_at_label,
    next2_requester_id,
    rows_in_priority_agent_id,
    distinct_priority_in_has_refund,
    requester_id_change,
    seq_no,
    first_requester_id,
    closed_at_rank_distance,
    requester_id_change_filled,
    distinct_priority_in_agent_id,
    requester_id_change_filled_share_of_agent_id
FROM metrics
ORDER BY seq_no
