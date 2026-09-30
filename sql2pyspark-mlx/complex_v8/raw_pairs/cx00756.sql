WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
        CASE WHEN requester_id >= 18 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM tickets
    WHERE agent_id <= 8
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label_2,
        LENGTH(TRIM(channel)) AS channel_len
    FROM filtered
    WHERE requester_id_tier NOT IN ('C', 'A')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        channel_prefix,
        channel_ref,
        requester_id_tier,
        opened_at_label_2,
        channel_len,
        NTILE(5) OVER (PARTITION BY agent_id, channel_len ORDER BY opened_at NULLS LAST, id) AS opened_at_quintile,
        SIZE(COLLECT_SET(requester_id_tier) OVER (PARTITION BY agent_id, channel)) AS distinct_requester_id_tier_in_agent_id_channel,
        MIN(requester_id) OVER (PARTITION BY csat ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far
    FROM annotated
),
agent_id_stats AS (
    -- Median opened_at_quintile per agent_id
    SELECT agent_id, PERCENTILE_APPROX(opened_at_quintile, 0.5) AS median_opened_at_quintile
    FROM ranked
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_opened_at_quintile,
        ROUND(s.opened_at_quintile - g.median_opened_at_quintile, 2) AS opened_at_quintile_minus_median
    FROM ranked s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.opened_at_quintile >= g.median_opened_at_quintile
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.channel_prefix,
        s.channel_ref,
        s.requester_id_tier,
        s.opened_at_label_2,
        s.opened_at_quintile,
        s.distinct_requester_id_tier_in_agent_id_channel,
        s.min_requester_id_so_far,
        s.median_opened_at_quintile,
        s.opened_at_quintile_minus_median,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM above_average s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND t.actor NOT IN ('agent', 'customer')
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    csat,
    channel_prefix,
    requester_id_tier,
    opened_at_quintile,
    distinct_requester_id_tier_in_agent_id_channel,
    min_requester_id_so_far,
    median_opened_at_quintile,
    opened_at_quintile_minus_median,
    event_id,
    event_actor,
    CASE WHEN requester_id_tier IN ('A', 'C') THEN 'group_a' WHEN requester_id_tier = 'B' THEN 'group_b' ELSE 'other' END AS requester_id_tier_group,
    LOWER(REGEXP_REPLACE(TRIM(event_actor), ' ', '_')) AS event_actor_slug
FROM line_level_events
ORDER BY id DESC, event_id
LIMIT 10
