WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
    FROM tickets
    WHERE csat < 3
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        SUM(CASE WHEN channel = 'chat' THEN 1 ELSE 0 END) AS n_channel_chat,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        ROUND(100 * COUNT(CASE WHEN priority = 'P2' THEN 1 END) / COUNT(*), 1) AS pct_priority_p2,
        MAX(opened_at) AS last_opened_at,
        ROUND(SUM(csat) / NULLIF(SUM(minutes_closed_at_to_opened_at), 0), 3) AS csat_to_minutes_closed_at_to_opened_at_ratio,
        SUM(minutes_closed_at_to_opened_at) AS total_minutes_closed_at_to_opened_at
    FROM scoped_tickets
    GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(opened_at_ym)) AS opened_at_ym_clean,
        pct_priority_p2 + n_channel_chat AS pct_priority_p2_plus_n_channel_chat
    FROM totals
),
tagged AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(last_opened_at, 'MMM yyyy') AS last_opened_at_label
    FROM scored
    WHERE MONTH(last_opened_at) IN (1, 2)
),
per_label AS (
    -- One row per channel_list_item
    SELECT
        s.opened_at_ym,
        s.n_channel_chat,
        s.pct_priority_p2,
        s.last_opened_at,
        s.csat_to_minutes_closed_at_to_opened_at_ratio,
        s.total_minutes_closed_at_to_opened_at,
        s.opened_at_ym_clean,
        s.pct_priority_p2_plus_n_channel_chat,
        s.last_opened_at_label,
        t.channel_list_item_pos,
        t.channel_list_item
    FROM tagged s
    LATERAL VIEW POSEXPLODE(s.channel_list) t AS channel_list_item_pos, channel_list_item
)
SELECT
    opened_at_ym,
    n_channel_chat,
    pct_priority_p2,
    last_opened_at,
    csat_to_minutes_closed_at_to_opened_at_ratio,
    total_minutes_closed_at_to_opened_at,
    pct_priority_p2_plus_n_channel_chat,
    last_opened_at_label,
    channel_list_item_pos,
    channel_list_item,
    ABS(n_channel_chat - 5) AS n_channel_chat_distance
FROM per_label
ORDER BY n_channel_chat_distance DESC, pct_priority_p2 DESC
