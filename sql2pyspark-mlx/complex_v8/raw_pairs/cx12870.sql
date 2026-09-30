WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref,
        opened_at + INTERVAL 7 DAYS AS opened_at_plus_7d,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'upgrade') OR agent_id IS NOT NULL)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY requester_id, id), 3) AS requester_id_percent_rank,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY channel ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        ROUND((UNIX_TIMESTAMP(opened_at_plus_7d) - UNIX_TIMESTAMP(LAG(opened_at_plus_7d) OVER w_channel_by_requester_id)) / 60, 1) AS minutes_since_prev_opened_at_plus_7d,
        NTH_VALUE(opened_at_plus_7d, 2) OVER (PARTITION BY channel ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_plus_7d
    FROM filtered
    WINDOW w_channel_by_requester_id AS (PARTITION BY channel ORDER BY requester_id, id)
),
with_activity AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team <> 'Onboarding'
        )
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    channel,
    priority_ref,
    opened_at_plus_7d,
    channel_prefix,
    requester_id_percent_rank,
    second_requester_id,
    minutes_since_prev_opened_at_plus_7d,
    SUBSTRING(TRIM(priority_ref), 1, 2) AS priority_ref_prefix,
    MONTH(second_opened_at_plus_7d) AS second_opened_at_plus_7d_month
FROM with_activity
ORDER BY priority DESC, priority_ref DESC
