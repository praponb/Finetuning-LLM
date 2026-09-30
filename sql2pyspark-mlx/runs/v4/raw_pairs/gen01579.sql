WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity / unit_price, 2) line_total
    FROM order_items
    WHERE unit_price BETWEEN 280 AND 350
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total DESC NULLS FIRST, id ASC) AS rn
    FROM filtered
    QUALIFY rn = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.category,
        b.launch_date
    FROM ranked a
    LEFT JOIN products b
        ON a.product_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY category ORDER BY unit_price ASC, id ASC) unit_price_bucket,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY category, order_id ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity,
        FIRST_VALUE(category, TRUE) OVER (PARTITION BY product_id, category ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_category,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity
    FROM enriched
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, d.amount
    FROM scored e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
joined_order_items AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.launch_date AS product_launch_date,
        p.brand
    FROM joined e
    LEFT JOIN products p
        ON e.product_id = p.id
),
joined_order_items_2 AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.stock_qty,
        d.cost
    FROM joined_order_items a
    INNER JOIN products d
        ON a.product_id = d.id
)
-- Multi-level aggregation
SELECT
    product_id,
    order_id,
    CASE WHEN unit_price_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_bucket_tier,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    RANK() OVER (PARTITION BY product_id ORDER BY COUNT(CASE WHEN brand = 'Stark' THEN 1 END) DESC) AS stark_count_rank
FROM joined_order_items_2
GROUP BY product_id, order_id, unit_price_bucket_tier
ORDER BY stark_count DESC NULLS LAST;
