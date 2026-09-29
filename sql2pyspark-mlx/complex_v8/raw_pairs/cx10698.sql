WITH recent_tickets AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE priority IN ('P2', 'P4')
      AND csat >= 2
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        priority,
        ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        COUNT(*) AS n_rows
    FROM recent_tickets
    GROUP BY priority
    HAVING SUM(requester_id) > 8
),
enriched AS (
    -- Label and score rows
    SELECT *, CONCAT(priority, '#', CAST(n_rows AS STRING)) AS priority_ref
    FROM grouped
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        priority,
        requester_id_to_csat_ratio,
        median_csat,
        n_rows,
        priority_ref,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER w_median_csat_priority, 0), 4) AS n_rows_share_of_median_csat_priority,
        LAST_VALUE(requester_id_to_csat_ratio) OVER (ORDER BY median_csat DESC, priority ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id_to_csat_ratio,
        MAX(requester_id_to_csat_ratio) OVER (ORDER BY median_csat DESC, priority ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_to_csat_ratio_max_2_prior,
        median_csat - LAG(median_csat) OVER (ORDER BY median_csat DESC, priority) AS median_csat_change,
        DENSE_RANK() OVER w_all_by_median_csat AS median_csat_dense_rank
    FROM enriched
    WINDOW w_median_csat_priority AS (PARTITION BY median_csat, priority), w_all_by_median_csat AS (ORDER BY median_csat DESC, priority)
)
SELECT *
FROM (
    SELECT median_csat_dense_rank, priority, median_csat
    FROM windowed
) src
PIVOT (
    COUNT(median_csat)
    FOR priority IN ('P4', 'P1', 'P2')
)
ORDER BY median_csat_dense_rank
