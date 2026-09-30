WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.tags_count,
        t.tag_pos,
        t.tag
    FROM base_tickets s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags_count,
        tag_pos,
        tag_pos - LAG(tag_pos) OVER (ORDER BY opened_at ASC, id, tag_pos) AS tag_pos_change,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY opened_at ASC, id, tag_pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at
    FROM flattened
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(opened_at) AS opened_at_date,
        LOWER(TRIM(channel)) AS channel_clean,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM windowed
)
-- Subtotals with GROUPING SETS
SELECT
    tag_pos,
    agent_id,
    COALESCE(channel, 'ALL') AS channel,
    SUM(CASE WHEN channel = 'phone' THEN tag_pos_change ELSE 0 END) AS tag_pos_change_channel_phone,
    PERCENTILE_APPROX(tag_pos_change, 0.5) AS median_tag_pos_change,
    COUNT(DISTINCT agent_id) AS distinct_agent_ids
FROM annotated
GROUP BY GROUPING SETS ((tag_pos, agent_id, channel), (tag_pos), ())
