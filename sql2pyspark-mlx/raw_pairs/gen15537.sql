WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(unit_price, 0) AS unit_price_filled,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_id,
        unit_price,
        unit_price_filled,
        quantity_filled
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price DESC, id) = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.unit_price,
        src.quantity_filled,
        b.brand
    FROM latest src
    LEFT JOIN products b
        ON src.product_id = b.id
    WHERE b.stock_qty <= 351
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        unit_price,
        quantity_filled,
        brand,
        AVG(quantity_filled) OVER w_running AS last3_avg_quantity_filled,
        MAX(quantity_filled) OVER w_running AS last3_max_quantity_filled,
        MIN(quantity_filled) OVER w_running AS last3_min_quantity_filled,
        SUM(quantity_filled) OVER w_running AS last3_sum_quantity_filled,
        ROW_NUMBER() OVER (ORDER BY quantity_filled DESC, id) AS seq_no
    FROM joined
    WINDOW w_running AS (PARTITION BY brand, product_id ORDER BY quantity_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(quantity_filled) OVER w_grp AS last7_count_quantity_filled,
        AVG(quantity_filled) OVER w_grp AS last7_avg_quantity_filled,
        MIN(quantity_filled) OVER w_grp AS last7_min_quantity_filled,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM scored
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY seq_no, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY brand ORDER BY seq_no, id)
)
-- Ranking and gap features
SELECT
    id,
    unit_price,
    quantity_filled,
    last3_max_quantity_filled,
    last7_count_quantity_filled,
    last7_min_quantity_filled,
    SUM(last7_min_quantity_filled) OVER w_hist AS running_last7_min_quantity_filled,
    LAG(last7_count_quantity_filled, 2) OVER w_hist_2 AS prev_last7_count_quantity_filled,
    SUM(event_seq) OVER w_hist_3 AS running_event_seq,
    NTILE(10) OVER w_grp AS seq_no_bucket,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY last3_min_quantity_filled DESC, id) AS event_seq_2
FROM scored_order_items
WINDOW w_hist AS (PARTITION BY product_id, brand ORDER BY last7_count_quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY product_id ORDER BY unit_price, id), w_hist_3 AS (PARTITION BY brand ORDER BY last3_avg_quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id, brand ORDER BY seq_no, id);
