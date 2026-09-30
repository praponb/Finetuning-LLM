WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price * 1.15, 2) AS unit_price_scaled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IN (17, 26, 39)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.*,
        b.category,
        b.cost,
        b.launch_date
    FROM filtered s
    LEFT JOIN products b
        ON s.product_id = b.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category, product_id ORDER BY unit_price, id) AS rank_in_group
    FROM with_dims
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC NULLS LAST, id) AS latest_rank
        FROM ranked
    ) x
    WHERE latest_rank = 1
),
ranked_order_items AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        category,
        launch_date,
        rank_in_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY rank_in_group DESC, id) AS dedup_rank
        FROM latest
    ) t
    WHERE dedup_rank = 1
)
-- Ranking and gap features
SELECT
    id,
    quantity,
    unit_price,
    launch_date,
    rank_in_group,
    FIRST_VALUE(category) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_category,
    ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY launch_date DESC, id) AS seq_no,
    LAG(quantity, 1) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS prev_quantity,
    NTILE(5) OVER (PARTITION BY YEAR(launch_date), category ORDER BY unit_price DESC, id) AS unit_price_bucket,
    NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date), order_id ORDER BY quantity, id) AS quantity_bucket
FROM ranked_order_items
