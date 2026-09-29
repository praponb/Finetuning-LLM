WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        GREATEST(revenue, units) AS greatest_revenue_units
    FROM store_sales
    WHERE units IS NOT NULL
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        COALESCE(greatest_revenue_units, 0) AS greatest_revenue_units_filled
    FROM valid
    WHERE sale_date >= DATE '2024-03-30'
      AND product_id IN (5, 6, 10)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        greatest_revenue_units_filled,
        LAST_VALUE(revenue, TRUE) OVER (ORDER BY greatest_revenue_units_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share
    FROM valid_store_sales
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        greatest_revenue_units_filled,
        last_known_revenue,
        revenue_share,
        SUM(greatest_revenue_units_filled) OVER (PARTITION BY product_id ORDER BY revenue_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_revenue_units_filled,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_known_revenue DESC, id) AS row_seq
    FROM with_windows
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(row_seq, 1) OVER (PARTITION BY product_id ORDER BY last_known_revenue, id) AS next_row_seq,
        AVG(last_known_revenue) OVER (PARTITION BY product_id ORDER BY revenue_share, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last_known_revenue_avg_6_prior,
        LEAD(revenue_share, 2, 0) OVER (PARTITION BY product_id ORDER BY last_known_revenue, id) AS next_revenue_share,
        SUM(revenue_share) OVER (PARTITION BY product_id ORDER BY greatest_revenue_units_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_share,
        LAST_VALUE(last_known_revenue) IGNORE NULLS OVER (ORDER BY last_known_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_revenue
    FROM enriched
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_known_revenue_avg_6_prior DESC, id) AS seq_no,
        DENSE_RANK() OVER w_running AS next_revenue_share_dense_rank
    FROM enriched_store_sales
    WINDOW w_running AS (PARTITION BY product_id ORDER BY next_revenue_share DESC)
)
SELECT
    s.id,
    s.product_id,
    s.sale_date,
    s.revenue,
    s.revenue_share,
    s.row_seq,
    s.last_known_revenue_avg_6_prior,
    s.next_revenue_share,
    s.running_revenue_share,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS sum_inventory_quantity
FROM with_windows_store_sales s
ORDER BY row_seq, sale_date;
