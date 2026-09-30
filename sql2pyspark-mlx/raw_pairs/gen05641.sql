WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        AVG(quantity) OVER w_hist AS quantity_avg_2,
        NTILE(10) OVER w_ordered AS quantity_bucket,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_percent_rank,
        SUM(quantity) OVER w_part AS running_quantity
    FROM inventory
    WINDOW w_hist AS (ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY product_id ORDER BY quantity, id), w_part AS (PARTITION BY YEAR(last_restock), product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(quantity_bucket) OVER w_recent AS prev_quantity_bucket,
        LAG(quantity_percent_rank) OVER w_recent AS prev_quantity_percent_rank,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM metrics
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY quantity_avg_2, id)
),
metrics_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_bucket,
        running_quantity,
        prev_quantity_bucket,
        MAX(quantity) OVER (PARTITION BY product_id) AS product_id_max_quantity,
        SUM(quantity) OVER (PARTITION BY YEAR(last_restock), product_id ORDER BY quantity_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        LAST_VALUE(quantity_avg_2) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('WEEK', last_restock), product_id ORDER BY prev_quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_avg_2
    FROM enriched
    WHERE seq_no >= 4
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.cost,
        p.price
    FROM metrics_inventory src
    LEFT JOIN products p
        ON src.product_id = p.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        running_quantity - prev_quantity_bucket AS running_quantity_minus_prev_quantity_bucket,
        ROUND(quantity, 0) AS quantity_rounded
    FROM joined
    WHERE prev_quantity_bucket = 6
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_bucket,
        prev_quantity_bucket,
        cumulative_quantity,
        running_quantity_minus_prev_quantity_bucket,
        quantity_rounded,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        AVG(cumulative_quantity) OVER (PARTITION BY product_id ORDER BY quantity_rounded, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cumulative_quantity_avg_5_prior,
        ROW_NUMBER() OVER (ORDER BY quantity_rounded DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_restock), product_id ORDER BY last_known_quantity_avg_2, id) AS row_seq
    FROM valid
)
SELECT
    id,
    quantity,
    last_restock,
    quantity_bucket,
    prev_quantity_bucket,
    cumulative_quantity,
    running_quantity_minus_prev_quantity_bucket,
    quantity_rounded,
    cumulative_quantity_avg_5_prior
FROM enriched_inventory;
