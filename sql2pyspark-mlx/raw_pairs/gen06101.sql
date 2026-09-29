WITH joined AS (
    -- Attach orders attributes
    SELECT s.*, o.order_date
    FROM order_items s
    INNER JOIN orders o
        ON s.order_id = o.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.launch_date,
        b.cost,
        b.product_name
    FROM joined a
    INNER JOIN products b
        ON a.product_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS prev_unit_price,
        LEAD(unit_price) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS next_unit_price,
        LEAD(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS next_quantity,
        LAG(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS row_seq
    FROM with_dims
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS row_seq_2,
        STDDEV_SAMP(next_quantity) OVER (PARTITION BY product_id, order_id) AS product_id_stddev_samp_next_quantity
    FROM scored
    WINDOW w_ordered AS (ORDER BY next_quantity ASC, id ASC)
),
scored_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY prev_unit_price ASC, id ASC) AS prev_unit_price_bucket,
        LEAD(next_unit_price, 2, 0) OVER (PARTITION BY YEAR(order_date) ORDER BY unit_price ASC, id ASC) AS next_next_unit_price,
        LAG(prev_quantity, 2) OVER (ORDER BY quantity ASC, id ASC) AS prev_prev_quantity
    FROM with_windows
),
scored_order_items_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY prev_prev_quantity DESC) AS prev_prev_quantity_cume_dist,
        ROUND(prev_prev_quantity / SUM(prev_prev_quantity) OVER (PARTITION BY order_id, product_id), 4) AS prev_prev_quantity_share
    FROM scored_order_items
    WHERE next_unit_price >= 190
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN cost >= 560 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        APPROX_PERCENTILE(prev_unit_price, ARRAY(0.25, 0.5, 0.75)) AS prev_unit_price_quartiles,
        BIT_OR(quantity) AS quantity_bit_or,
        APPROX_PERCENTILE(prev_prev_quantity_share, 0.9) AS prev_prev_quantity_share_p90
    FROM scored_order_items_2
    GROUP BY CASE WHEN cost >= 560 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    cost_tier,
    prev_unit_price_quartiles,
    prev_prev_quantity_share_p90
FROM daily
