WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS FIRST, id) AS row_num
    FROM inventory
    QUALIFY row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS quantity_bucket,
        AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_avg_6,
        PERCENT_RANK() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC NULLS LAST) AS quantity_percent_rank
    FROM latest
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.product_name,
        p.price,
        p.launch_date
    FROM metrics e
    LEFT JOIN products p
        ON e.product_id = p.id
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category,
        p.cost,
        p.price AS product_price
    FROM with_dims src
    LEFT JOIN products p
        ON src.product_id = p.id
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        DATE_TRUNC('MONTH', last_restock) AS activity_month,
        product_id,
        ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS price_per_grocery,
        ROUND(SUM(quantity_bucket) / NULLIF(COUNT(CASE WHEN category = 'Sports' THEN 1 END), 0), 2) AS quantity_bucket_per_sports,
        SIZE(COLLECT_SET(category)) AS num_categories,
        PERCENTILE_APPROX(quantity_bucket, ARRAY(0.25, 0.5, 0.75)) AS quantity_bucket_quartiles,
        COUNT(*) AS num_rows
    FROM joined
    GROUP BY DATE_TRUNC('WEEK', launch_date), DATE_TRUNC('MONTH', last_restock), product_id
    HAVING num_rows < 1
)
SELECT
    week_start,
    activity_month,
    product_id,
    price_per_grocery,
    quantity_bucket_per_sports,
    num_categories,
    quantity_bucket_quartiles,
    num_rows
FROM summary
ORDER BY num_categories NULLS LAST;
