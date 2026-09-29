WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE (premium > 1042.8 OR holder_id IS NOT NULL)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY YEAR(end_date), status ORDER BY start_date ASC, id) AS start_date_tercile,
        DENSE_RANK() OVER (PARTITION BY YEAR(end_date), status ORDER BY start_date ASC) AS start_date_dense_rank,
        LAG(end_date) OVER (PARTITION BY YEAR(end_date), status ORDER BY start_date ASC, id) AS prev_end_date
    FROM filtered
)
-- Subtotals with ROLLUP
SELECT
    status,
    holder_id,
    GROUPING(status) AS is_total_status,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    ROUND(100 * COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) / COUNT(*), 1) AS pct_status_active
FROM metrics
GROUP BY ROLLUP(status, holder_id)
