WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE product NOT IN ('HEALTH', 'LIFE')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_status_product_by_start_date AS seq_no,
        FIRST_VALUE(start_date) OVER (PARTITION BY status, product ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        ROUND(CUME_DIST() OVER (PARTITION BY status, product ORDER BY start_date DESC, id), 3) AS start_date_cume_dist,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY status, product ORDER BY start_date DESC, id)) AS days_since_prev_start_date
    FROM recent_policys
    WINDOW w_status_product_by_start_date AS (PARTITION BY status, product ORDER BY start_date DESC, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        seq_no,
        first_start_date,
        start_date_cume_dist,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        SUM(days_since_prev_start_date) OVER (PARTITION BY status, holder_id) AS status_holder_id_sum_days_since_prev_start_date
    FROM sequenced
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(status, 'ALL') AS status,
    seq_no,
    COALESCE(product, 'ALL') AS product,
    COUNT(*) AS cnt,
    MIN(start_date_cume_dist) AS min_start_date_cume_dist,
    MAX(start_date) AS last_start_date
FROM ranked
GROUP BY ROLLUP(status, seq_no, product)
