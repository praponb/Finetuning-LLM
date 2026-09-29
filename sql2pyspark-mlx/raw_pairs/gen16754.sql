WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(price) OVER w_hist AS running_sum_price,
        COUNT(price) OVER w_hist AS running_count_price,
        MIN(price) OVER w_hist AS running_min_price
    FROM products
    WINDOW w_hist AS (PARTITION BY supplier_id, category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        running_count_price,
        running_min_price,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY running_count_price, id) AS event_seq,
        NTILE(5) OVER (ORDER BY running_count_price DESC, id) AS running_count_price_bucket,
        SUM(running_sum_price) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_price
    FROM metrics
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.price,
        s.cost,
        s.launch_date,
        s.supplier_id,
        s.running_count_price,
        s.running_min_price,
        s.event_seq,
        s.running_count_price_bucket,
        p2.country,
        p2.rating
    FROM enriched s
    LEFT JOIN suppliers p2
        ON s.supplier_id <=> p2.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id, id
    FROM with_dims
    EXCEPT
    SELECT supplier_id, id
    FROM products
    WHERE launch_date > '2025-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN kept_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
),
enriched_products AS (
    -- Running and moving aggregates
    SELECT
        price,
        running_count_price,
        running_min_price,
        running_count_price_bucket,
        DENSE_RANK() OVER w_grp AS cost_dense_rank,
        ROUND(running_count_price / SUM(running_count_price) OVER (PARTITION BY country, supplier_id), 4) AS running_count_price_share,
        ROUND(running_count_price_bucket / SUM(running_count_price_bucket) OVER (PARTITION BY supplier_id), 4) AS running_count_price_bucket_share,
        MIN(running_min_price) OVER (PARTITION BY MONTH(launch_date)) AS launch_date_month_min_running_min_price,
        MIN(running_count_price) OVER (PARTITION BY country, supplier_id) AS country_min_running_count_price
    FROM kept_rows
    WINDOW w_grp AS (ORDER BY cost NULLS LAST)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        price,
        running_min_price,
        running_count_price_share,
        running_count_price_bucket_share,
        launch_date_month_min_running_min_price,
        ROUND(country_min_running_count_price, 0) AS country_min_running_count_price_rounded
    FROM enriched_products
    WHERE cost_dense_rank = 7
)
-- Grouped metrics
SELECT
    CASE WHEN launch_date_month_min_running_min_price >= 1313 THEN 'LARGE' ELSE 'SMALL' END AS launch_date_month_min_running_min_price_tier,
    CASE WHEN running_min_price >= 1000 THEN 'HIGH' ELSE 'LOW' END AS running_min_price_tier,
    CASE WHEN running_count_price_bucket_share >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_price_bucket_share_tier,
    COUNT(*) AS num_rows,
    ROUND(AVG(running_count_price_share), 2) AS avg_running_count_price_share
FROM cleaned
GROUP BY launch_date_month_min_running_min_price_tier, running_min_price_tier, running_count_price_bucket_share_tier
ORDER BY running_min_price_tier DESC NULLS FIRST;
