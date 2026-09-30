WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id, product_id), 4) quantity_share,
        LEAD(quantity, 1) OVER (PARTITION BY order_id ORDER BY quantity, id) next_quantity
    FROM order_items
    WHERE product_id IS NULL
),
with_windows_order_items AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        quantity_share,
        FIRST_VALUE(next_quantity) IGNORE NULLS OVER w_grp first_known_next_quantity,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY quantity NULLS FIRST) quantity_percent_rank,
        SUM(next_quantity) OVER w_grp running_next_quantity
    FROM with_windows
    WINDOW w_grp AS (ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        d.cost,
        d.stock_qty,
        d.brand
    FROM with_windows_order_items e
    JOIN products d
        ON e.product_id <=> d.id
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        quantity_share,
        first_known_next_quantity,
        running_next_quantity,
        brand,
        AVG(cost) OVER w_part cost_avg_3,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY running_next_quantity DESC NULLS FIRST) running_next_quantity_percent_rank,
        AVG(running_next_quantity) OVER (PARTITION BY brand, order_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) running_next_quantity_avg_6_prior
    FROM enriched
    WINDOW w_part AS (PARTITION BY brand, product_id ORDER BY running_next_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost_avg_3, id) AS rn
        FROM enriched_order_items
    ) x
    WHERE rn = 1
),
enriched_order_items_2 AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.brand,
        e.cost_avg_3,
        e.running_next_quantity_percent_rank,
        b.brand AS product_brand,
        b.stock_qty
    FROM deduped e
    LEFT JOIN products b
        ON e.product_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost_avg_3 >= 765 THEN 'LARGE' ELSE 'SMALL' END AS cost_avg_3_tier,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    ROUND(AVG(cost_avg_3), 2) AS avg_cost_avg_3,
    ROUND(SUM(cost_avg_3) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS cost_avg_3_per_acme
FROM enriched_order_items_2
GROUP BY CASE WHEN cost_avg_3 >= 765 THEN 'LARGE' ELSE 'SMALL' END
HAVING distinct_product_ids >= 1
ORDER BY cost_avg_3_tier DESC NULLS LAST;
