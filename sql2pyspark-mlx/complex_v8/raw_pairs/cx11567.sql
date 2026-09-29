WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        tags,
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE (closed_at >= TIMESTAMP '2025-02-02 03:00:00' OR channel = 'chat')
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM recent_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id BETWEEN 2 AND 25
        )
      AND s.priority IN ('P3', 'P1', 'P2')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        opened_at,
        priority_channel_label,
        RANK() OVER (PARTITION BY priority, agent_id ORDER BY opened_at, id) AS opened_at_rank,
        LEAD(opened_at, 3) OVER (ORDER BY opened_at, id) AS next3_opened_at
    FROM with_activity
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        opened_at_rank,
        MIN(opened_at) AS first_opened_at,
        PERCENTILE_APPROX(opened_at_rank, 0.5) AS median_opened_at_rank,
        ROUND(AVG(opened_at_rank), 2) AS avg_opened_at_rank,
        COUNT(DISTINCT id) AS distinct_ids
    FROM windowed
    GROUP BY opened_at_rank
    HAVING AVG(opened_at_rank) > 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        opened_at_rank,
        first_opened_at,
        median_opened_at_rank,
        distinct_ids,
        MAX(avg_opened_at_rank) OVER (PARTITION BY median_opened_at_rank) AS median_opened_at_rank_max_avg_opened_at_rank,
        MAX(opened_at_rank) OVER w_median_opened_at_rank AS median_opened_at_rank_max_opened_at_rank,
        SUM(avg_opened_at_rank) OVER (PARTITION BY median_opened_at_rank) AS median_opened_at_rank_sum_avg_opened_at_rank,
        NTILE(3) OVER w_first_opened_at_month_median AS first_opened_at_tercile,
        SUM(opened_at_rank) OVER (PARTITION BY MONTH(first_opened_at), median_opened_at_rank ORDER BY first_opened_at, opened_at_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_opened_at_rank,
        ROW_NUMBER() OVER w_first_opened_at_month_median AS seq_no
    FROM totals
    WINDOW w_median_opened_at_rank AS (PARTITION BY median_opened_at_rank), w_first_opened_at_month_median AS (PARTITION BY MONTH(first_opened_at), median_opened_at_rank ORDER BY first_opened_at, opened_at_rank)
),
first_rows AS (
    -- First row per group
    SELECT
        opened_at_rank,
        first_opened_at,
        median_opened_at_rank,
        distinct_ids,
        median_opened_at_rank_max_avg_opened_at_rank,
        median_opened_at_rank_max_opened_at_rank,
        median_opened_at_rank_sum_avg_opened_at_rank,
        first_opened_at_tercile,
        running_opened_at_rank
    FROM with_trends
    WHERE seq_no = 1
      AND HOUR(first_opened_at) BETWEEN 10 AND 14
),
enriched AS (
    -- Add calculated columns
    SELECT *, FLOOR(median_opened_at_rank / 2) * 2 AS median_opened_at_rank_bucket
    FROM first_rows
)
SELECT
    opened_at_rank,
    first_opened_at,
    median_opened_at_rank,
    distinct_ids,
    median_opened_at_rank_max_avg_opened_at_rank,
    median_opened_at_rank_max_opened_at_rank,
    median_opened_at_rank_sum_avg_opened_at_rank,
    first_opened_at_tercile,
    median_opened_at_rank_bucket
FROM enriched
