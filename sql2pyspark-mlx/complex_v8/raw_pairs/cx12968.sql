WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags,
        csat + requester_id AS csat_plus_requester_id,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        requester_id - csat AS requester_id_minus_csat
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'login')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(csat_per_requester_id, 2) OVER (PARTITION BY priority ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_per_requester_id,
        FIRST_VALUE(csat_plus_requester_id) IGNORE NULLS OVER (PARTITION BY priority ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat_plus_requester_id,
        ROUND(csat_plus_requester_id / NULLIF(SUM(csat_plus_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id), 0), 4) AS csat_plus_requester_id_share_of_opened_at_month_,
        RANK() OVER (PARTITION BY agent_id ORDER BY requester_id_minus_csat) AS requester_id_minus_csat_rank
    FROM scoped
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COUNT(*) FILTER (WHERE priority = 'P4') AS n_priority_p4,
        MAX(csat_plus_requester_id) AS max_csat_plus_requester_id,
        SUM(CASE WHEN priority IN ('P1', 'P4') THEN 1 ELSE 0 END) AS n_priority_in_p1_p4,
        COUNT(*) AS n_rows
    FROM ranked
    GROUP BY DATE_TRUNC('MONTH', opened_at)
    HAVING COUNT(*) >= 3
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(n_priority_p4, 2) OVER (ORDER BY max_csat_plus_requester_id DESC, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_priority_p4,
        NTILE(3) OVER (ORDER BY max_csat_plus_requester_id DESC, opened_at_month) AS max_csat_plus_requester_id_tercile
    FROM per_group
)
-- Final projection
SELECT
    opened_at_month,
    n_priority_p4,
    n_priority_in_p1_p4,
    n_rows,
    second_n_priority_p4,
    max_csat_plus_requester_id_tercile,
    n_rows + n_priority_p4 AS n_rows_plus_n_priority_p4,
    LEAST(n_priority_p4, n_rows) AS least_n_priority_p4_n_rows
FROM metrics
ORDER BY n_rows NULLS LAST, n_rows_plus_n_priority_p4 DESC
