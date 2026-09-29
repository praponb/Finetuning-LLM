WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(cost, 0) AS cost_filled
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'DHL'
      AND cost BETWEEN 44 AND 88
)
-- Ranking and gap features
SELECT
    id,
    shipped_date,
    delivered_date,
    status,
    cost_filled,
    COUNT(cost) OVER w_hist AS running_count_cost,
    SUM(cost) OVER w_hist AS running_sum_cost,
    MIN(cost) OVER w_hist AS running_min_cost,
    AVG(cost) OVER w_hist AS running_avg_cost,
    PERCENT_RANK() OVER w_ordered AS cost_percent_rank
FROM valid
WHERE shipped_date >= '2023-11-01'
WINDOW w_hist AS (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY status ORDER BY cost DESC NULLS FIRST)
