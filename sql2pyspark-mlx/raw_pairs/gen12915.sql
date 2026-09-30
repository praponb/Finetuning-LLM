WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label
    FROM payments
    WHERE paid_at >= DATE '2023-08-16'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount) OVER w_running AS running_max_amount,
        MIN(amount) OVER w_running AS running_min_amount,
        SUM(amount) OVER w_running AS running_sum_amount
    FROM base
    WINDOW w_running AS (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, b.status AS order_status
    FROM metrics src
    INNER JOIN orders b
        ON src.order_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM enriched
    INTERSECT ALL
    SELECT id, status
    FROM payments
    WHERE method = 'cash'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
)
-- Grouped metrics
SELECT
    CASE WHEN running_sum_amount >= 8300 THEN 'HIGH' ELSE 'LOW' END AS running_sum_amount_tier,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    APPROX_PERCENTILE(running_sum_amount, 0.95) AS running_sum_amount_p95,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_rows,
    MAX(amount) AS max_amount,
    COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count,
    MIN(running_sum_amount) AS min_running_sum_amount,
    GROUPING_ID() AS grouping_level
FROM kept_rows
GROUP BY CUBE (CASE WHEN running_sum_amount >= 8300 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', paid_at), DATE_TRUNC('YEAR', paid_at))
HAVING min_running_sum_amount > 0
ORDER BY running_sum_amount_tier NULLS FIRST
