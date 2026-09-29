WITH latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        top1.launch_date,
        top1.product_price
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.launch_date, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        launch_date,
        product_price,
        MIN(product_price) OVER (ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_product_price,
        SUM(product_price) OVER (ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_product_price,
        MAX(product_price) OVER (ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_product_price
    FROM latest_match
    WHERE YEAR(launch_date) = 2024
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        launch_date,
        product_price,
        running_sum_product_price,
        running_max_product_price,
        MIN(product_price) OVER w_recent AS running_min_product_price_2,
        SUM(product_price) OVER w_recent AS running_sum_product_price_2,
        MAX(product_price) OVER w_recent AS running_max_product_price_2,
        AVG(product_price) OVER w_recent AS running_avg_product_price
    FROM enriched
    WINDOW w_recent AS (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM suppliers
    WHERE rating <= 3.2
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        launch_date,
        product_price,
        running_sum_product_price,
        running_max_product_price,
        running_min_product_price_2,
        running_sum_product_price_2,
        running_max_product_price_2,
        running_avg_product_price,
        GREATEST(running_sum_product_price_2, running_max_product_price) AS greatest_running_sum_product_price_2_running_max_product_price
    FROM kept_rows
    WHERE running_min_product_price_2 <= 780
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date >= s.launch_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= s.launch_date
        )
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        PERCENTILE_APPROX(running_sum_product_price, 0.95) AS running_sum_product_price_p95,
        APPROX_PERCENTILE(running_max_product_price, 0.95) AS running_max_product_price_p95,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(running_min_product_price_2, 0.5) AS running_min_product_price_2_p50,
        APPROX_PERCENTILE(running_max_product_price_2, 0.5) AS running_max_product_price_2_p50,
        APPROX_PERCENTILE(running_sum_product_price_2, 0.5) AS running_sum_product_price_2_p50
    FROM eligible
    GROUP BY DATE_TRUNC('WEEK', launch_date)
    HAVING running_sum_product_price_p95 > 0
)
SELECT
    week_start,
    running_sum_product_price_p95,
    running_max_product_price_p95,
    running_min_product_price_2_p50,
    running_max_product_price_2_p50,
    running_sum_product_price_2_p50
FROM summary;
