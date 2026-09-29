WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'bug') OR HOUR(closed_at) BETWEEN 6 AND 10)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat ORDER BY requester_id DESC, id), 3) AS requester_id_percent_rank,
        priority_len - LAG(priority_len) OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS priority_len_change
    FROM cleaned
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.priority_len,
        s.channel_or_unknown,
        s.priority_clean,
        s.requester_id_percent_rank,
        s.priority_len_change,
        t.tag
    FROM ranked s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        priority_len,
        channel_or_unknown,
        priority_clean,
        requester_id_percent_rank,
        tag,
        CASE WHEN requester_id >= 17 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        closed_at + INTERVAL 2 HOURS AS closed_at_plus_2h
    FROM exploded
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_requester_id
    FROM tagged s
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    csat,
    priority_len,
    priority_clean,
    closed_at_plus_2h,
    min_ticket_requester_id,
    COALESCE(csat, 0) AS csat_filled
FROM with_counts
ORDER BY agent_id DESC NULLS FIRST
