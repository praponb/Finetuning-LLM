WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, product_id
    FROM store_sales
    INTERSECT
    SELECT store_id, product_id
    FROM store_sales
    WHERE store_id IN (1, 3, 5, 6)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.brand,
        p.price
    FROM reconciled e
    LEFT JOIN products p
        ON e.product_id = p.id
),
with_dims_store_sales AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.price AS product_price,
        p.launch_date,
        p.stock_qty
    FROM with_dims e
    INNER JOIN products p
        ON e.product_id = p.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        s.region,
        s.city,
        s.store_name
    FROM with_dims_store_sales e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY brand ORDER BY revenue DESC NULLS FIRST) AS revenue_percent_rank,
        CORR(revenue, units) OVER (PARTITION BY region) AS corr_revenue_units
    FROM enriched
),
with_dims_store_sales_2 AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.category,
        p.cost,
        p.launch_date AS product_launch_date
    FROM enriched_store_sales e
    LEFT JOIN products p
        ON e.product_id = p.id
),
enriched_store_sales_2 AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.product_name,
        b.brand AS product_brand
    FROM with_dims_store_sales_2 e
    LEFT JOIN products b
        ON e.product_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN product_price >= 1070 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_price_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    PERCENTILE_APPROX(product_price, 0.95) AS product_price_p95,
    ROUND(SUM(corr_revenue_units) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS corr_revenue_units_per_stark
FROM enriched_store_sales_2
GROUP BY CASE WHEN product_price >= 1070 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(SUM(corr_revenue_units) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) > 0
ORDER BY distinct_store_ids DESC NULLS FIRST;
