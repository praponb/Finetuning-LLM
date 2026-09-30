WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
        MAX(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        MIN(quantity) OVER (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM inventory
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        last_restock,
        running_sum_quantity,
        running_max_quantity,
        quantity_avg_6_prior,
        RANK() OVER (PARTITION BY store_id ORDER BY running_sum_quantity DESC) AS running_sum_quantity_rank,
        RANK() OVER (PARTITION BY store_id ORDER BY running_sum_quantity) AS running_sum_quantity_rank_2,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior_2
    FROM enriched
    WHERE running_sum_quantity <= 570
),
enriched_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        last_restock,
        running_max_quantity,
        quantity_avg_6_prior,
        running_sum_quantity_rank,
        SUM(running_sum_quantity) OVER (PARTITION BY product_id ORDER BY running_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_sum_quantity,
        AVG(running_sum_quantity) OVER (PARTITION BY product_id ORDER BY running_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_sum_quantity,
        MIN(running_sum_quantity) OVER (PARTITION BY product_id ORDER BY running_max_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_sum_quantity,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('month', last_restock) ORDER BY quantity_avg_6_prior DESC, id) AS quantity_avg_6_prior_bucket
    FROM scored
),
enriched_inventory_2 AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.running_sum_quantity_rank,
        e.running_sum_running_sum_quantity,
        e.running_avg_running_sum_quantity,
        e.quantity_avg_6_prior_bucket,
        b.category
    FROM enriched_inventory e
    INNER JOIN products b
        ON e.product_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    category,
    CASE WHEN running_avg_running_sum_quantity >= 1080 THEN 'HIGH' ELSE 'LOW' END AS running_avg_running_sum_quantity_tier,
    APPROX_PERCENTILE(quantity_avg_6_prior_bucket, ARRAY(0.25, 0.5, 0.75)) AS quantity_avg_6_prior_bucket_quartiles,
    COUNT_IF(category = 'Home') AS home_events
FROM enriched_inventory_2
GROUP BY category, CASE WHEN running_avg_running_sum_quantity >= 1080 THEN 'HIGH' ELSE 'LOW' END
ORDER BY category NULLS LAST;
