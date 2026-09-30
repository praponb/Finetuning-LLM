WITH prepared AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE requester_id <= 23
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.tier AS agent_tier
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier = 'T1'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        agent_tier,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM joined
    WHERE agent_id IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        csat,
        agent_tier,
        channel,
        ARRAY_SORT(COLLECT_SET(agent_tier)) AS agent_tier_list,
        COUNT(*) AS n_rows
    FROM scored
    GROUP BY csat, agent_tier, channel
),
flattened AS (
    -- One row per agent_tier_list_item
    SELECT
        s.csat,
        s.agent_tier,
        s.channel,
        s.n_rows,
        t.agent_tier_list_item_pos,
        t.agent_tier_list_item
    FROM rollup_base s
    LATERAL VIEW OUTER POSEXPLODE(s.agent_tier_list) t AS agent_tier_list_item_pos, agent_tier_list_item
),
tagged AS (
    -- Label and score rows
    SELECT
        csat,
        agent_tier,
        channel,
        agent_tier_list_item_pos,
        agent_tier_list_item,
        LOWER(REGEXP_REPLACE(TRIM(agent_tier), ' ', '_')) AS agent_tier_slug,
        LOWER(REGEXP_REPLACE(TRIM(agent_tier_list_item), ' ', '_')) AS agent_tier_list_item_slug
    FROM flattened
    WHERE n_rows <= 11
)
SELECT
    csat,
    agent_tier,
    agent_tier_list_item_pos,
    agent_tier_list_item,
    agent_tier_slug,
    agent_tier_list_item_slug,
    ROUND(csat / NULLIF(agent_tier_list_item_pos, 0), 3) AS csat_per_agent_tier_list_item_pos,
    CONCAT_WS(' / ', agent_tier, channel) AS agent_tier_channel_label
FROM tagged
WHERE agent_tier_list_item_pos <= 3
ORDER BY agent_tier_slug DESC, csat_per_agent_tier_list_item_pos DESC NULLS FIRST
