WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        QUARTER(closed_at) AS closed_at_quarter,
        CONCAT_WS('-', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-26'
      AND priority = 'P2'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(channel_priority_label)) AS channel_priority_label_clean,
        ROUND(requester_id_filled / NULLIF(closed_at_quarter, 0), 3) AS requester_id_filled_per_closed_at_quarter,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM recent
    WHERE requester_id < 18
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.requester_id_filled,
        s.closed_at_quarter,
        s.channel_priority_label,
        s.channel_priority_label_clean,
        s.requester_id_filled_per_closed_at_quarter,
        s.days_since_opened_at,
        t.tag_pos,
        t.tag
    FROM enriched s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
)
SELECT
    id,
    agent_id,
    opened_at,
    requester_id_filled,
    closed_at_quarter,
    channel_priority_label,
    channel_priority_label_clean,
    requester_id_filled_per_closed_at_quarter,
    days_since_opened_at,
    tag,
    CASE WHEN requester_id >= 18 THEN 'large' WHEN requester_id >= 9 THEN 'medium' ELSE 'small' END AS requester_id_tier
FROM exploded
ORDER BY id NULLS LAST
