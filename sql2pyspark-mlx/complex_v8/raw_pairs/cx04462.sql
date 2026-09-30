WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        LOWER(TRIM(priority)) AS priority_clean,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE agent_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(closed_at) OVER w_channel_opened_at_month_by_o AS next_closed_at,
        LEAD(csat, 3) OVER (PARTITION BY MONTH(closed_at), channel ORDER BY opened_at ASC, id) AS next3_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority), 2) AS csat_vs_priority_avg,
        NTILE(5) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at NULLS LAST, id) AS opened_at_quintile,
        ROW_NUMBER() OVER w_agent_id_by_csat AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY MONTH(closed_at), channel ORDER BY opened_at ASC, id) AS rn,
        ROUND(csat / NULLIF(SUM(csat) OVER w_agent_id, 0), 4) AS csat_share_of_agent_id,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at
    FROM cleaned
    WINDOW w_channel_opened_at_month_by_o AS (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at NULLS LAST, id), w_agent_id_by_csat AS (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id), w_agent_id AS (PARTITION BY agent_id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        priority_clean,
        channel_slug,
        next_closed_at,
        next3_csat,
        csat_vs_priority_avg,
        opened_at_quintile,
        rn,
        csat_share_of_agent_id,
        rows_in_priority,
        second_opened_at
    FROM sequenced
    WHERE seq_no <= 2
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at,
    priority,
    channel,
    csat,
    channel_slug,
    next_closed_at,
    next3_csat,
    opened_at_quintile,
    csat_share_of_agent_id,
    ROUND((UNIX_TIMESTAMP(second_opened_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_second_opened_at,
    CONCAT_WS(' / ', channel_slug, priority_clean) AS channel_slug_priority_clean_label
FROM top_per_group
ORDER BY next_closed_at NULLS LAST, id DESC
