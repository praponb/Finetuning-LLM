WITH prepared AS (
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
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY priority, csat) AS rows_in_priority_csat,
        MIN(requester_id) OVER (PARTITION BY priority, csat) AS priority_csat_min_requester_id,
        ROUND(PERCENT_RANK() OVER w_channel_by_closed_at, 3) AS closed_at_percent_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority, csat), 2) AS csat_vs_priority_csat_avg
    FROM prepared
    WINDOW w_channel_by_closed_at AS (PARTITION BY channel ORDER BY closed_at ASC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(rows_in_priority_csat - AVG(rows_in_priority_csat) OVER (PARTITION BY priority, csat), 2) AS rows_in_priority_csat_vs_priority_csat_avg,
        LAST_VALUE(requester_id) IGNORE NULLS OVER (ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id,
        ROUND(csat_vs_priority_csat_avg - AVG(csat_vs_priority_csat_avg) OVER (PARTITION BY priority, csat), 2) AS csat_vs_priority_csat_avg_vs_priority_csat_avg
    FROM with_windows
),
deduped AS (
    -- Keep the latest row per agent_id
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
        rows_in_priority_csat,
        priority_csat_min_requester_id,
        closed_at_percent_rank,
        csat_vs_priority_csat_avg,
        rows_in_priority_csat_vs_priority_csat_avg,
        last_known_requester_id,
        csat_vs_priority_csat_avg_vs_priority_csat_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY priority_csat_min_requester_id DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT channel, opened_at, priority, last_known_requester_id
    FROM deduped
) src
PIVOT (
    SUM(last_known_requester_id)
    FOR priority IN ('P1', 'P2', 'P3', 'P4')
)
ORDER BY channel, opened_at
