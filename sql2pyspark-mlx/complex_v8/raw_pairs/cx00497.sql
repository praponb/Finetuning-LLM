WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        CASE WHEN requester_id >= 21 THEN 'A' WHEN requester_id >= 8 THEN 'B' ELSE 'C' END AS requester_id_tier,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE (closed_at < TIMESTAMP '2025-02-20 15:00:00' OR priority = 'P2')
),
summary AS (
    -- Summarise per group
    SELECT
        channel,
        agent_id,
        SUM(CASE WHEN priority = 'P2' THEN 1 ELSE 0 END) AS n_priority_p2,
        MIN(csat) AS min_csat
    FROM cleaned_tickets
    GROUP BY 1, 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(min_csat) OVER (ORDER BY min_csat DESC NULLS FIRST, channel, agent_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS min_csat_sum_7,
        MAX(min_csat) OVER w_min_csat_n_priority_p2 AS min_csat_n_priority_p2_max_min_csat
    FROM summary
    WINDOW w_min_csat_n_priority_p2 AS (PARTITION BY min_csat, n_priority_p2)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN channel IN ('phone', 'chat') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM with_trends
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT min_csat, channel, min_csat_sum_7
    FROM filtered
) src
PIVOT (
    SUM(min_csat_sum_7)
    FOR channel IN ('email', 'phone')
)
