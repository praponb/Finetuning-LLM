WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        CAST(quantity AS STRING) AS quantity_num
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity = 5
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(quantity) OVER w_recent AS last3_count_quantity,
        MIN(quantity) OVER w_recent AS last3_min_quantity,
        SUM(quantity) OVER w_recent AS last3_sum_quantity,
        MAX(quantity) OVER w_recent AS last3_max_quantity,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM valid
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.quantity,
        a.last3_count_quantity,
        a.last3_min_quantity,
        a.last3_max_quantity,
        a.last_known_quantity,
        o.status,
        o.order_date,
        o.amount
    FROM metrics a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_grp AS amount_bucket,
        LAG(quantity, 1) OVER w_grp_2 AS prev_quantity,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY quantity DESC) AS quantity_dense_rank,
        AVG(last3_max_quantity) OVER w_hist AS last3_max_quantity_avg_5
    FROM joined
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY amount, id), w_grp_2 AS (PARTITION BY status ORDER BY last3_count_quantity, id), w_hist AS (PARTITION BY status ORDER BY last3_max_quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows
    FROM with_windows
    GROUP BY ROLLUP (CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING COUNT(DISTINCT order_id) > 1
)
SELECT quantity_tier, pending_rows
FROM stats
ORDER BY quantity_tier DESC NULLS LAST;
