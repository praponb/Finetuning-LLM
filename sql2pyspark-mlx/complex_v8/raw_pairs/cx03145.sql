WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug_2,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND agent_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', channel_slug, channel_slug_2) AS channel_slug_channel_slug_2_label
    FROM cleaned_tickets
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority_or_unknown, channel ORDER BY opened_at, id), 3) AS opened_at_percent_rank,
        DENSE_RANK() OVER w_priority_or_unknown_agent_id AS opened_at_dense_rank,
        LAG(closed_at) OVER (PARTITION BY priority_or_unknown, agent_id ORDER BY opened_at DESC, id) AS prev_closed_at
    FROM scored
    WINDOW w_priority_or_unknown_agent_id AS (PARTITION BY priority_or_unknown, agent_id ORDER BY opened_at ASC)
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.channel_slug,
        s.channel_slug_2,
        s.priority_or_unknown,
        s.channel_slug_channel_slug_2_label,
        s.opened_at_percent_rank,
        s.opened_at_dense_rank,
        s.prev_closed_at,
        t.tag_pos,
        t.tag
    FROM metrics s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
),
derived AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label
    FROM per_label
)
SELECT
    id,
    agent_id,
    opened_at,
    channel_slug,
    channel_slug_2,
    channel_slug_channel_slug_2_label,
    opened_at_percent_rank,
    opened_at_dense_rank,
    tag_pos,
    tag,
    TO_DATE(prev_closed_at) AS prev_closed_at_date
FROM derived
ORDER BY opened_at DESC, channel_slug_2 DESC NULLS FIRST, id, tag_pos
LIMIT 20
