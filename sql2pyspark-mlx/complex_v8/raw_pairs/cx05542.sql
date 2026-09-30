WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE MONTH(opened_at) IN (2, 3)
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.requester_id_bucket,
        su.agent_name,
        su.team AS agent_team
    FROM scoped s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        channel,
        requester_id_bucket,
        agent_team,
        COUNT(*) OVER w_agent_team_csat AS rows_in_agent_team_csat,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY channel)) AS distinct_agent_team_in_channel,
        MAX(requester_id_bucket) OVER (PARTITION BY agent_team, csat) AS agent_team_csat_max_requester_id_bucket,
        LAST_VALUE(csat) OVER (PARTITION BY channel, csat ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        ROW_NUMBER() OVER (PARTITION BY agent_team, csat ORDER BY requester_id DESC, id) AS requester_id_seq,
        NTILE(10) OVER w_all_by_requester_id_bucket AS requester_id_bucket_decile,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_team, csat)) AS distinct_agent_id_in_agent_team_csat,
        ROW_NUMBER() OVER (ORDER BY csat DESC, id) AS rn,
        LAST_VALUE(requester_id_bucket) IGNORE NULLS OVER (PARTITION BY channel, csat ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_bucket
    FROM with_details_agent
    WINDOW w_agent_team_csat AS (PARTITION BY agent_team, csat), w_all_by_requester_id_bucket AS (ORDER BY requester_id_bucket DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        requester_id_bucket,
        agent_team,
        rows_in_agent_team_csat,
        distinct_agent_team_in_channel,
        agent_team_csat_max_requester_id_bucket,
        requester_id_bucket_decile,
        distinct_agent_id_in_agent_team_csat,
        last_known_requester_id_bucket,
        GREATEST(requester_id_bucket, requester_id_seq) AS greatest_requester_id_bucket_requester_id_seq,
        FLOOR(requester_id_bucket / 5) * 5 AS requester_id_bucket_bucket,
        FLOOR(rn / 2) * 2 AS rn_bucket
    FROM ranked
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_team,
        rows_in_agent_team_csat,
        requester_id_bucket_decile,
        distinct_agent_id_in_agent_team_csat,
        last_known_requester_id_bucket,
        greatest_requester_id_bucket_requester_id_seq,
        requester_id_bucket_bucket,
        rn_bucket,
        NTILE(10) OVER (PARTITION BY requester_id_bucket_decile, distinct_agent_team_in_channel ORDER BY distinct_agent_team_in_channel DESC, id) AS distinct_agent_team_in_channel_decile,
        LEAD(requester_id_bucket_decile, 2) OVER (PARTITION BY requester_id_bucket_decile, distinct_agent_team_in_channel ORDER BY distinct_agent_team_in_channel DESC, id) AS next2_requester_id_bucket_decile,
        SUM(requester_id_bucket_decile) OVER (PARTITION BY requester_id_bucket_decile, distinct_agent_team_in_channel ORDER BY distinct_agent_team_in_channel DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_bucket_decile_sum_7
    FROM annotated
)
-- Final projection
SELECT
    id,
    agent_team,
    rows_in_agent_team_csat,
    requester_id_bucket_decile,
    distinct_agent_id_in_agent_team_csat,
    last_known_requester_id_bucket,
    greatest_requester_id_bucket_requester_id_seq,
    requester_id_bucket_bucket,
    distinct_agent_team_in_channel_decile,
    next2_requester_id_bucket_decile,
    requester_id_bucket_decile_sum_7,
    CASE WHEN agent_team IN ('Onboarding', 'VIP') THEN 'group_a' WHEN agent_team = 'Billing' THEN 'group_b' ELSE 'other' END AS agent_team_group
FROM with_windows
ORDER BY rows_in_agent_team_csat DESC NULLS FIRST, distinct_agent_team_in_channel_decile
