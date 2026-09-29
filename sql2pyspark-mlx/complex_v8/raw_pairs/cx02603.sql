WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
rollup_base AS (
    -- Group-level totals
    SELECT
        prior_rows_3d,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        ROUND(STDDEV(prior_rows_3d), 2) AS stddev_prior_rows_3d,
        COUNT(DISTINCT id) AS distinct_ids
    FROM joined_ranges
    GROUP BY prior_rows_3d, DATE_TRUNC('MONTH', opened_at)
)
-- Subtotals with CUBE
SELECT
    prior_rows_3d,
    distinct_ids,
    MAX(opened_at_month) AS last_opened_at_month,
    COUNT(*) AS cnt
FROM rollup_base
GROUP BY CUBE(prior_rows_3d, distinct_ids)
ORDER BY prior_rows_3d NULLS LAST, distinct_ids NULLS LAST
