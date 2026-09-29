WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        UPPER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE channel IN ('phone', 'chat', 'web')
      AND priority = 'P1'
),
unique_rows AS (
    -- Keep the latest row per priority, channel
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        channel_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, channel ORDER BY csat DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_clean,
        t.tag
    FROM unique_rows s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
)
SELECT
    COALESCE(priority, 'ALL') AS priority,
    csat,
    COALESCE(channel, 'ALL') AS channel,
    GROUPING(priority) AS is_total_priority,
    COUNT(*) FILTER (WHERE channel IN ('web', 'phone', 'chat')) AS n_channel_in_web_phone_chat,
    MIN(csat) AS min_csat
FROM exploded
GROUP BY ROLLUP(priority, csat, channel)
ORDER BY priority NULLS LAST, csat NULLS LAST, channel NULLS LAST
