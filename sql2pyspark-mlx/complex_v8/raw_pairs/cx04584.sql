WITH prepared_tickets AS (
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
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE agent_id >= 7
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.priority_slug,
        s.channel_len,
        su.tier AS agent_tier
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel_len ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(AVG(csat) OVER (PARTITION BY YEAR(opened_at), csat), 2) AS opened_at_year_csat_avg_csat,
        LAG(opened_at) OVER w_channel_len_by_requester_id AS prev_opened_at,
        ROUND(channel_len - AVG(channel_len) OVER w_opened_at_year_csat, 2) AS channel_len_vs_opened_at_year_csat_avg,
        LAST_VALUE(csat) OVER (PARTITION BY csat ORDER BY channel_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        COUNT(*) OVER w_opened_at_year_csat AS rows_in_opened_at_year_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY YEAR(opened_at), csat)) AS distinct_agent_id_in_opened_at_year_csat,
        SUM(channel_len) OVER w_opened_at_year_csat AS opened_at_year_csat_sum_channel_len
    FROM with_parent
    WINDOW w_channel_len_by_requester_id AS (PARTITION BY channel_len ORDER BY requester_id, id), w_opened_at_year_csat AS (PARTITION BY YEAR(opened_at), csat)
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    priority_slug,
    channel_len,
    agent_tier,
    second_closed_at,
    opened_at_year_csat_avg_csat,
    channel_len_vs_opened_at_year_csat_avg,
    last_csat,
    rows_in_opened_at_year_csat,
    distinct_agent_id_in_opened_at_year_csat,
    opened_at_year_csat_sum_channel_len,
    DATEDIFF(DATE '2025-12-31', prev_opened_at) AS days_since_prev_opened_at,
    YEAR(second_closed_at) AS second_closed_at_year
FROM windowed
WHERE requester_id < 20
ORDER BY opened_at_year_csat_avg_csat, last_csat DESC, id
LIMIT 20
