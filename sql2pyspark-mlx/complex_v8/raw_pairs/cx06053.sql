WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE csat BETWEEN 2 AND 5
      AND MONTH(closed_at) IN (1, 3)
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.priority_or_unknown,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE priority <> 'P1') AS n_priority_p1,
        COUNT(*) FILTER (WHERE agent_tier IN ('T3', 'T2')) AS n_agent_tier_in_t3_t2,
        COUNT(*) AS row_count
    FROM screened
    WHERE closed_at >= TIMESTAMP '2025-01-24 01:00:00'
    GROUP BY DATE_TRUNC('MONTH', opened_at)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        opened_at_month,
        distinct_ids,
        n_priority_p1,
        n_agent_tier_in_t3_t2,
        row_count,
        ROUND(n_priority_p1 - AVG(n_priority_p1) OVER (PARTITION BY n_priority_p1, row_count), 2) AS n_priority_p1_vs_n_priority_p1_row_count_avg,
        ROUND(n_agent_tier_in_t3_t2 / NULLIF(SUM(n_agent_tier_in_t3_t2) OVER (PARTITION BY n_priority_p1, row_count), 0), 4) AS n_agent_tier_in_t3_t2_share_of_n_priority_p1_row
    FROM rollup_base
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_windows
    WHERE n_agent_tier_in_t3_t2_share_of_n_priority_p1_row > (SELECT MIN(n_agent_tier_in_t3_t2_share_of_n_priority_p1_row) FROM with_windows)
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(n_agent_tier_in_t3_t2) OVER (PARTITION BY distinct_ids, n_agent_tier_in_t3_t2 ORDER BY n_agent_tier_in_t3_t2_share_of_n_priority_p1_row, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_n_agent_tier_in_t3_t2_so_far,
        ROUND(AVG(n_agent_tier_in_t3_t2) OVER (PARTITION BY distinct_ids), 2) AS distinct_ids_avg_n_agent_tier_in_t3_t2
    FROM without_matches
)
SELECT
    opened_at_month,
    distinct_ids,
    n_priority_p1,
    n_agent_tier_in_t3_t2,
    n_priority_p1_vs_n_priority_p1_row_count_avg,
    n_agent_tier_in_t3_t2_share_of_n_priority_p1_row,
    min_n_agent_tier_in_t3_t2_so_far,
    distinct_ids_avg_n_agent_tier_in_t3_t2,
    ABS(n_agent_tier_in_t3_t2 - 3) AS n_agent_tier_in_t3_t2_distance
FROM with_windows_2
ORDER BY n_agent_tier_in_t3_t2_share_of_n_priority_p1_row, distinct_ids_avg_n_agent_tier_in_t3_t2 DESC NULLS FIRST
