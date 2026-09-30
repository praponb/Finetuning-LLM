WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        tags,
        CASE WHEN category IN ('Fashion', 'Home') THEN 'Tier 1' ELSE 'Other' END AS category_group,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE launch_date >= DATE '2024-01-30'
      AND supplier_id IN (1, 5, 38)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(cost, 1) OVER w_hist AS next_cost,
        FIRST_VALUE(brand) IGNORE NULLS OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand,
        LAST_VALUE(cost, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        NTILE(10) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        SUM(price) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price
    FROM base
    WINDOW w_hist AS (PARTITION BY category, brand ORDER BY price, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY brand ORDER BY running_price, id) AS running_price_bucket,
        SUM(last_known_cost) OVER w_running AS cumulative_last_known_cost
    FROM enriched
    WINDOW w_running AS (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock BETWEEN '2023-07-01' AND '2024-08-01'
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT brand
    FROM eligible
    EXCEPT ALL
    SELECT brand
    FROM products
    WHERE supplier_id = 13
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.brand = k.brand
),
scored_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY category, brand), 4) AS cost_share,
        PERCENT_RANK() OVER (ORDER BY cumulative_last_known_cost DESC) AS cumulative_last_known_cost_percent_rank,
        CUME_DIST() OVER (PARTITION BY category ORDER BY next_cost DESC NULLS LAST) AS next_cost_cume_dist,
        SUM(running_price_bucket) OVER (PARTITION BY brand, category) AS brand_sum_running_price_bucket,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share_2
    FROM reconciled
)
SELECT
    product_name,
    category,
    brand,
    price,
    cost,
    category_group,
    next_cost,
    first_known_brand,
    last_known_cost
FROM scored_products
ORDER BY cost DESC;
