WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(revenue, 0) AS revenue_filled,
        COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE sale_date > DATE '2023-09-27'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT store_id, product_id
    FROM filtered
    INTERSECT
    SELECT store_id, product_id
    FROM store_sales
    WHERE revenue < 3300
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        store_id,
        product_id,
        sale_date,
        revenue_filled,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM reconciled
    WHERE store_id IN (1, 3, 5, 6)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        store_id,
        product_id,
        revenue_filled,
        AVG(revenue_filled) OVER (PARTITION BY store_id, product_id) AS store_id_avg_revenue_filled,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue_filled NULLS FIRST) AS revenue_filled_rank,
        PERCENT_RANK() OVER (ORDER BY revenue_filled NULLS FIRST) AS revenue_filled_percent_rank,
        AVG(revenue_filled) OVER (PARTITION BY product_id) AS product_id_avg_revenue_filled
    FROM base
    WHERE store_id = 6
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.product_id,
        s.store_id_avg_revenue_filled,
        s.revenue_filled_percent_rank,
        s.product_id_avg_revenue_filled,
        b.price
    FROM metrics s
    JOIN products b
        ON s.product_id = b.id
)
-- Multi-level aggregation
SELECT
    product_id,
    CASE WHEN product_id_avg_revenue_filled >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_avg_revenue_filled_tier,
    PERCENTILE_APPROX(store_id_avg_revenue_filled, 0.9) AS store_id_avg_revenue_filled_p90,
    APPROX_PERCENTILE(price, 0.9) AS price_p90,
    PERCENTILE_APPROX(revenue_filled_percent_rank, 0.9) AS revenue_filled_percent_rank_p90,
    MIN(price) AS min_price,
    APPROX_PERCENTILE(product_id_avg_revenue_filled, 0.5) AS product_id_avg_revenue_filled_p50,
    COUNT(*) AS num_rows
FROM enriched
GROUP BY GROUPING SETS ((product_id, CASE WHEN product_id_avg_revenue_filled >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END), (product_id))
HAVING PERCENTILE_APPROX(revenue_filled_percent_rank, 0.9) > 0;
