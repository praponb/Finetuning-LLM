WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue - units revenue_per_unit,
        CEIL(revenue) revenue_whole
    FROM store_sales
    WHERE sale_date > DATE '2023-08-24'
      AND MONTH(sale_date) IN (1, 4, 8)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(revenue_whole) OVER (PARTITION BY store_id) store_id_max_revenue_whole,
        LAG(revenue_per_unit, 2) OVER (PARTITION BY store_id ORDER BY units, id) prev_revenue_per_unit,
        AVG(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) units_avg_2_prior
    FROM base
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue_per_unit,
        prev_revenue_per_unit,
        units_avg_2_prior,
        LEAST(revenue_per_unit, prev_revenue_per_unit) least_revenue_per_unit_prev_revenue_per_unit
    FROM enriched
    WHERE product_id IN (3, 10)
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.revenue_per_unit,
        a.prev_revenue_per_unit,
        a.least_revenue_per_unit_prev_revenue_per_unit,
        d.cost,
        d.launch_date,
        d.price
    FROM filtered a
    INNER JOIN products d
        ON a.product_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER w_running prev_revenue_per_unit_percent_rank,
        NTILE(4) OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit, id) revenue_per_unit_bucket,
        AVG(revenue_per_unit) OVER w_hist revenue_per_unit_avg_5_prior
    FROM enriched_store_sales
    WINDOW w_running AS (PARTITION BY DATE_TRUNC('week', launch_date), store_id ORDER BY prev_revenue_per_unit DESC NULLS LAST), w_hist AS (PARTITION BY product_id, store_id ORDER BY prev_revenue_per_unit, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    SUM(cost) AS total_cost,
    MAX(prev_revenue_per_unit) AS max_prev_revenue_per_unit,
    MAX(revenue_per_unit) AS max_revenue_per_unit
FROM scored
GROUP BY DATE_TRUNC('YEAR', launch_date)
HAVING max_revenue_per_unit < 1000000
ORDER BY total_cost DESC;
