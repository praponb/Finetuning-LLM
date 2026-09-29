WITH valid_tickets AS (
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
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE (closed_at < TIMESTAMP '2025-02-24 13:00:00' OR SIZE(tags) >= 2)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        closed_at,
        channel,
        csat,
        tags,
        requester_id_bucket,
        LAG(requester_id_bucket, 2) OVER (ORDER BY csat, id) AS prev2_requester_id_bucket,
        LAG(closed_at, 3) OVER (PARTITION BY MONTH(opened_at) ORDER BY requester_id_bucket ASC, id) AS prev3_closed_at,
        LEAD(opened_at) OVER (ORDER BY csat, id) AS next_opened_at,
        ROUND(requester_id_bucket / NULLIF(SUM(requester_id_bucket) OVER w_agent_id_csat, 0), 4) AS requester_id_bucket_share_of_agent_id_csat
    FROM valid_tickets
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat)
)
-- Subtotals with ROLLUP
SELECT
    csat,
    channel,
    GROUPING_ID() AS gid,
    ROUND(SUM(prev2_requester_id_bucket) / NULLIF(SUM(requester_id), 0), 3) AS prev2_requester_id_bucket_to_requester_id_ratio,
    SUM(requester_id) AS total_requester_id
FROM with_trends
GROUP BY ROLLUP(csat, channel)
ORDER BY csat NULLS LAST, channel NULLS LAST
