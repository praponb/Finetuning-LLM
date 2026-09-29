WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        CASE WHEN requester_id >= 16 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM tickets
    WHERE MONTH(closed_at) IN (2, 3)
      AND priority IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek,
        COALESCE(csat, 0) AS csat_filled
    FROM recent_tickets
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at >= TIMESTAMP '2025-02-08 15:00:00'
        )
      AND s.csat >= 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, csat_filled ORDER BY csat_filled DESC), 3) AS csat_filled_percent_rank,
        RANK() OVER (PARTITION BY csat, csat_filled ORDER BY csat_filled DESC, id) AS csat_filled_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY csat, csat_filled ORDER BY csat_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at
    FROM without_matches
)
SELECT
    csat,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    GROUPING(csat) AS is_total_csat,
    ROUND(AVG(csat_filled_percent_rank), 2) AS avg_csat_filled_percent_rank
FROM sequenced
GROUP BY CUBE(csat, requester_id_tier)
