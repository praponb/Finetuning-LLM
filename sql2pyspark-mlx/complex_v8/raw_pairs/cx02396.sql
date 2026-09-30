WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        INITCAP(TRIM(priority)) AS priority_clean,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE (channel IN ('chat', 'web', 'phone') OR agent_id >= 6)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(days_since_opened_at / NULLIF(requester_id, 0), 3) AS days_since_opened_at_per_requester_id,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM base_tickets
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.priority_clean,
        s.days_since_opened_at,
        s.channel_priority_label,
        s.days_since_opened_at_per_requester_id,
        s.minutes_opened_at_to_closed_at,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM scored s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(channel_priority_label, '#', CAST(requester_id AS STRING)) AS channel_priority_label_ref
    FROM enriched
    WHERE days_since_opened_at < 420
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        priority_clean,
        days_since_opened_at,
        channel_priority_label,
        days_since_opened_at_per_requester_id,
        minutes_opened_at_to_closed_at,
        agent_hired_on,
        agent_name,
        channel_priority_label_ref,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)) AS rows_in_agent_hired_on_month,
        DENSE_RANK() OVER (PARTITION BY agent_id, MONTH(opened_at) ORDER BY opened_at) AS opened_at_dense_rank
    FROM annotated
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY opened_at_dense_rank, YEAR(opened_at)), 0), 4) AS requester_id_share_of_opened_at_dense_rank_opene,
        SIZE(COLLECT_SET(priority) OVER w_opened_at_dense_rank_opened) AS distinct_priority_in_opened_at_dense_rank_opened,
        LAST_VALUE(days_since_opened_at) IGNORE NULLS OVER (PARTITION BY priority, MONTH(opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_opened_at,
        ROUND(days_since_opened_at - AVG(days_since_opened_at) OVER w_opened_at_dense_rank_opened, 2) AS days_since_opened_at_vs_opened_at_dense_rank_ope,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER w_opened_at_dense_rank_opened, 0), 4) AS days_since_opened_at_share_of_opened_at_dense_ra
    FROM with_windows
    WINDOW w_opened_at_dense_rank_opened AS (PARTITION BY opened_at_dense_rank, YEAR(opened_at))
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    days_since_opened_at_per_requester_id,
    minutes_opened_at_to_closed_at,
    agent_hired_on,
    channel_priority_label_ref,
    rows_in_agent_hired_on_month,
    requester_id_share_of_opened_at_dense_rank_opene,
    distinct_priority_in_opened_at_dense_rank_opened,
    last_known_days_since_opened_at,
    days_since_opened_at_vs_opened_at_dense_rank_ope,
    days_since_opened_at_share_of_opened_at_dense_ra,
    CONCAT(agent_name, '#', CAST(opened_at_dense_rank AS STRING)) AS agent_name_ref,
    CONCAT_WS('-', channel_priority_label, agent_name) AS channel_priority_label_agent_name_label
FROM metrics
WHERE opened_at >= TIMESTAMP '2025-01-25 11:00:00'
