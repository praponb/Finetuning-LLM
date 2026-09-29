WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        cost,
        supplier_id,
        AVG(cost) OVER (PARTITION BY YEAR(launch_date), brand ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) cost_avg_3_prior,
        AVG(price) OVER w_ordered price_avg_3,
        ROW_NUMBER() OVER w_grp seq_no
    FROM products
    WHERE supplier_id IS NOT NULL
    WINDOW w_ordered AS (PARTITION BY brand ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY cost DESC, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cost >= 700 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    supplier_id,
    MAX(price_avg_3) FILTER (WHERE category = 'Fashion') AS fashion_max_price_avg_3,
    MIN(cost_avg_3_prior) AS min_cost_avg_3_prior,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM metrics
GROUP BY cost_tier, supplier_id
HAVING fashion_max_price_avg_3 < 1000000;
