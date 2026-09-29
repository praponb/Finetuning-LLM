WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        csat + requester_id AS csat_plus_requester_id,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
        MONTH(opened_at) AS opened_at_month
    FROM tickets
    WHERE requester_id > 4
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        csat_plus_requester_id,
        priority_prefix,
        opened_at_month,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(closed_at) ORDER BY csat, id), 3) AS csat_percent_rank
    FROM base
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY rows_in_agent_id_csat ASC, id) AS rn,
        FIRST_VALUE(csat_plus_requester_id) IGNORE NULLS OVER (PARTITION BY agent_id, priority ORDER BY rows_in_agent_id_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat_plus_requester_id,
        MAX(csat_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), priority) AS closed_at_month_priority_max_csat_percent_rank,
        FIRST_VALUE(closed_at) OVER (PARTITION BY agent_id, priority ORDER BY rows_in_agent_id_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at
    FROM windowed
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        ROUND(csat_percent_rank / NULLIF(rn, 0), 3) AS csat_percent_rank_per_rn
    FROM with_windows
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at,
        (SELECT SUM(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS total_ticket_csat
    FROM scored s
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY csat_percent_rank, id) AS seq_no,
        MIN(rn) OVER (PARTITION BY priority_group, YEAR(first_closed_at)) AS priority_group_first_closed_at_year_min_rn,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY priority_group, YEAR(first_closed_at))) AS distinct_channel_in_priority_group_first_closed_,
        NTH_VALUE(opened_at_month, 2) OVER (PARTITION BY opened_at_month, agent_id ORDER BY csat_plus_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_month,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority_group, YEAR(first_closed_at))) AS distinct_agent_id_in_priority_group_first_closed
    FROM profiled
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, csat_percent_rank, closed_at_month_priority_max_csat_percent_rank
    FROM metrics
) src
UNPIVOT (
    value FOR field IN (csat_percent_rank, closed_at_month_priority_max_csat_percent_rank)
) unp
ORDER BY id, field
