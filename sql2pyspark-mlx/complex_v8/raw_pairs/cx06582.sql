WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date
    FROM policies
    WHERE product IN ('TRAVEL', 'LIFE')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(days_since_end_date) OVER (PARTITION BY product ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_end_date,
        ROUND(AVG(days_since_end_date) OVER (PARTITION BY product ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_since_end_date_avg_4,
        COUNT(*) OVER w_product_holder_id AS rows_in_product_holder_id
    FROM scoped_policys
    WINDOW w_product_holder_id AS (PARTITION BY product, holder_id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        days_since_end_date,
        running_days_since_end_date,
        rows_in_product_holder_id,
        LAG(end_date) OVER (ORDER BY days_since_end_date ASC, id) AS prev_end_date,
        COUNT(*) OVER (PARTITION BY holder_id, product) AS rows_in_holder_id_product,
        NTILE(3) OVER (ORDER BY days_since_end_date ASC, id) AS days_since_end_date_tercile,
        FIRST_VALUE(days_since_end_date_avg_4) IGNORE NULLS OVER (ORDER BY days_since_end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_end_date_avg_4
    FROM sequenced
)
-- Subtotals with ROLLUP
SELECT
    days_since_end_date_tercile,
    holder_id,
    product,
    GROUPING(days_since_end_date_tercile) AS is_total_days_since_end_date_tercile,
    COUNT(*) AS n_rows
FROM with_windows
GROUP BY ROLLUP(days_since_end_date_tercile, holder_id, product)
ORDER BY days_since_end_date_tercile NULLS LAST, holder_id NULLS LAST, product NULLS LAST
