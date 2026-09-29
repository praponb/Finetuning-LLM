WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
    WHERE requester_id > 7
      AND csat >= 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROW_NUMBER() OVER w_all_by_closed_at AS rn,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        MAX(csat) OVER w_priority_csat AS priority_csat_max_csat,
        SUM(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        COUNT(*) OVER (PARTITION BY priority, csat) AS rows_in_priority_csat,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY priority, csat)) AS distinct_channel_in_priority_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat,
        SUM(csat) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_2,
        ROUND(csat - AVG(csat) OVER w_csat, 2) AS csat_vs_csat_avg,
        NTILE(10) OVER (ORDER BY closed_at DESC, id) AS closed_at_decile
    FROM filtered_tickets
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at DESC, id), w_priority_csat AS (PARTITION BY priority, csat), w_csat AS (PARTITION BY csat)
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    rows_in_priority_csat,
    distinct_channel_in_priority_csat,
    csat_share_of_csat,
    running_csat_2,
    csat_vs_csat_avg,
    closed_at_decile
FROM sequenced
WHERE rows_in_priority_csat >= 6
ORDER BY running_csat_2, priority NULLS LAST, id
LIMIT 10
