WITH filtered AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        LENGTH(product_name) AS product_name_length
    FROM products
    WHERE launch_date > TO_DATE('2023-08-07')
),
matched_keys AS (
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM products
    WHERE launch_date > '2024-01-01'
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    SELECT *, PERCENT_RANK() OVER w_part AS cost_percent_rank
    FROM verified
    WINDOW w_part AS (PARTITION BY category ORDER BY cost DESC NULLS FIRST)
),
cleaned AS (
    SELECT *, ROUND(price, 1) AS price_rounded
    FROM enriched
    WHERE brand IS DISTINCT FROM 'Acme'
),
enriched_products AS (
    SELECT
        id,
        category,
        product_name_length,
        price_rounded,
        MIN(cost_percent_rank) OVER w_part AS brand_min_cost_percent_rank,
        AVG(price_rounded) OVER w_hist AS brand_avg_price_rounded,
        SUM(price) OVER w_hist AS brand_sum_price
    FROM cleaned
    WHERE category IN ('Electronics', 'Home', 'Grocery')
    WINDOW w_part AS (PARTITION BY brand), w_hist AS (PARTITION BY brand, category)
),
with_windows AS (
    SELECT
        id,
        category,
        product_name_length,
        price_rounded,
        brand_avg_price_rounded,
        brand_sum_price,
        CUME_DIST() OVER w_running AS brand_avg_price_rounded_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY brand_sum_price DESC NULLS LAST) AS brand_sum_price_percent_rank,
        RANK() OVER w_running AS brand_avg_price_rounded_rank,
        RANK() OVER w_recent AS brand_min_cost_percent_rank_rank
    FROM enriched_products
    WINDOW w_running AS (ORDER BY brand_avg_price_rounded DESC NULLS FIRST), w_recent AS (PARTITION BY category ORDER BY brand_min_cost_percent_rank DESC)
)
SELECT
    id,
    category,
    price_rounded,
    brand_avg_price_rounded_cume_dist,
    brand_sum_price_percent_rank,
    brand_avg_price_rounded_rank,
    brand_min_cost_percent_rank_rank,
    CASE WHEN category RLIKE '^(Electronics|Grocery)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS category_zone
FROM with_windows
ORDER BY category DESC, brand_sum_price_percent_rank;
