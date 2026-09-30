WITH joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        b.order_date,
        b.shipping_fee
    FROM order_items s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
joined_order_items AS (
    -- Attach products attributes
    SELECT a.*, b.product_name
    FROM joined a
    LEFT JOIN products b
        ON a.product_id = b.id
    WHERE LOWER(b.product_name) LIKE '%oga%'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        order_date,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
        SUM(shipping_fee) OVER w_grp AS cumulative_shipping_fee
    FROM joined_order_items
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        cumulative_quantity,
        cumulative_shipping_fee,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity_avg_2 DESC) AS quantity_avg_2_percent_rank,
        LAG(cumulative_quantity, 2) OVER (PARTITION BY order_id ORDER BY cumulative_shipping_fee, id) AS prev_cumulative_quantity,
        MIN(cumulative_quantity) OVER w_part AS order_id_min_cumulative_quantity
    FROM enriched
    WINDOW w_part AS (PARTITION BY order_id)
),
joined_order_items_2 AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.cumulative_quantity,
        a.quantity_avg_2_percent_rank,
        a.prev_cumulative_quantity,
        a.order_id_min_cumulative_quantity,
        p.brand,
        p.cost
    FROM metrics a
    LEFT JOIN products p
        ON a.product_id <=> p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity_avg_2_percent_rank >= 0.68 THEN 'HIGH' ELSE 'LOW' END AS quantity_avg_2_percent_rank_tier,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    APPROX_PERCENTILE(cumulative_quantity, ARRAY(0.25, 0.5, 0.75)) AS cumulative_quantity_quartiles
FROM joined_order_items_2
GROUP BY CASE WHEN quantity_avg_2_percent_rank >= 0.68 THEN 'HIGH' ELSE 'LOW' END
ORDER BY quantity_avg_2_percent_rank_tier DESC NULLS FIRST;
