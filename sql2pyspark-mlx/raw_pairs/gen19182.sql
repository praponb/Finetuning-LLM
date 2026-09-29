WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        p.city,
        p.rating
    FROM products e
    INNER JOIN suppliers p
        ON e.supplier_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        launch_date,
        tags,
        city,
        rating,
        MAX(rating) OVER w_running AS last7_max_rating,
        MIN(rating) OVER w_running AS last7_min_rating,
        AVG(rating) OVER w_running AS last7_avg_rating,
        SUM(rating) OVER w_running AS last7_sum_rating,
        CORR(cost, price) OVER w_ordered AS corr_cost_price
    FROM joined
    WINDOW w_running AS (PARTITION BY city ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY MONTH(launch_date), supplier_id)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id IN (20, 40)
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(last7_max_rating) OVER (PARTITION BY city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last7_max_rating,
        MAX(last7_max_rating) OVER (PARTITION BY city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last7_max_rating,
        COUNT(last7_max_rating) OVER (PARTITION BY city ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last7_max_rating
    FROM kept
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (2, 8, 9)
        )
),
enriched_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(stock_qty) OVER w_hist AS next_stock_qty,
        LEAD(last7_max_rating) OVER w_hist AS next_last7_max_rating,
        ROW_NUMBER() OVER w_hist AS seq_no,
        SUM(rating) OVER (PARTITION BY category ORDER BY last7_max_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM not_flagged
    WHERE city IN ('Chiang Mai', 'London')
    WINDOW w_hist AS (PARTITION BY category, city ORDER BY last7_avg_rating, id)
)
SELECT
    id,
    product_name,
    cost,
    stock_qty,
    launch_date,
    tags,
    city,
    rating,
    last7_max_rating,
    CASE WHEN product_name RLIKE '^(Office Chair|Desk Lamp)$' THEN 'OTHER_ZONE' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS product_name_zone
FROM enriched_products
ORDER BY cost DESC NULLS LAST;
