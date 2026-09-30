WITH enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.price,
        p.category,
        p.brand
    FROM order_items src
    INNER JOIN products p
        ON src.product_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        category,
        brand,
        MIN(price) OVER (PARTITION BY category ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_price,
        COUNT(price) OVER (PARTITION BY category ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_price,
        SUM(price) OVER (PARTITION BY category ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_price,
        AVG(price) OVER (PARTITION BY category ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_price,
        AVG(price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS price_avg_5
    FROM enriched
    WHERE product_id = 40
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        category,
        running_min_price,
        running_sum_price,
        SUM(price_avg_5) OVER (PARTITION BY brand) AS brand_sum_price_avg_5,
        AVG(running_avg_price) OVER (PARTITION BY category ORDER BY running_sum_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_avg_price_avg_2_prior,
        SUM(running_avg_price) OVER (PARTITION BY brand ORDER BY running_sum_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_avg_price,
        SUM(price_avg_5) OVER (PARTITION BY brand, product_id ORDER BY running_sum_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price_avg_5,
        AVG(running_sum_price) OVER (ORDER BY running_count_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_sum_price_avg_5_prior
    FROM metrics
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_running_avg_price) OVER (PARTITION BY category ORDER BY cumulative_price_avg_5, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_running_avg_price_avg_5_prior,
        NTILE(10) OVER (PARTITION BY category ORDER BY running_sum_price, id) AS running_sum_price_bucket,
        FIRST_VALUE(cumulative_price_avg_5) IGNORE NULLS OVER (PARTITION BY category ORDER BY cumulative_price_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_price_avg_5,
        LEAD(cumulative_price_avg_5, 1) OVER (PARTITION BY category ORDER BY running_sum_price, id) AS next_cumulative_price_avg_5
    FROM enriched_order_items
)
SELECT
    s.id,
    s.product_id,
    s.running_min_price,
    s.running_sum_price,
    s.brand_sum_price_avg_5,
    s.running_avg_price_avg_2_prior,
    s.running_running_avg_price,
    s.cumulative_price_avg_5,
    s.running_running_avg_price_avg_5_prior,
    (SELECT SUM(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS sum_inventory_quantity
FROM scored s
