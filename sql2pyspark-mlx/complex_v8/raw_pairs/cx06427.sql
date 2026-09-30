WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        COALESCE(csat, 0) AS csat_filled,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE requester_id >= 7
      AND opened_at < TIMESTAMP '2025-03-02 08:00:00'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        ROUND(SUM(closed_at_hour) / NULLIF(SUM(csat_filled), 0), 3) AS closed_at_hour_to_csat_filled_ratio,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        COUNT(*) FILTER (WHERE channel IN ('chat', 'web', 'phone')) AS n_channel_in_chat_web_phone,
        COUNT(*) AS cnt
    FROM recent_tickets
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER w_n_channel_in_chat_web_phone AS cnt_decile,
        DENSE_RANK() OVER (PARTITION BY cnt, distinct_agent_ids ORDER BY closed_at_hour_to_csat_filled_ratio) AS closed_at_hour_to_csat_filled_ratio_dense_rank
    FROM totals
    WINDOW w_n_channel_in_chat_web_phone AS (PARTITION BY n_channel_in_chat_web_phone ORDER BY cnt ASC, opened_at_month)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE closed_at_hour_to_csat_filled_ratio < (SELECT MAX(closed_at_hour_to_csat_filled_ratio) FROM sequenced)
)
-- Native SQL UNPIVOT (wide to long)
SELECT opened_at_month, metric, value
FROM (
    SELECT opened_at_month, distinct_agent_ids, closed_at_hour_to_csat_filled_ratio_dense_rank, cnt
    FROM without_matches
) src
UNPIVOT (
    value FOR metric IN (distinct_agent_ids, closed_at_hour_to_csat_filled_ratio_dense_rank, cnt)
) unp
