WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        SUM(quantity) OVER w_ordered AS running_sum_quantity,
        AVG(quantity) OVER w_ordered AS running_avg_quantity,
        MIN(quantity) OVER w_ordered AS running_min_quantity,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS unit_price_bucket
    FROM valid
    WINDOW w_ordered AS (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_order_items AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        running_avg_quantity,
        unit_price_bucket,
        ROUND(unit_price_bucket / SUM(unit_price_bucket) OVER (PARTITION BY order_id), 4) AS unit_price_bucket_share,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_bucket, id) AS event_seq
    FROM enriched
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.shipping_fee,
        p.discount,
        p.amount
    FROM enriched_order_items e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM joined
    INTERSECT
    SELECT order_id, id
    FROM order_items
    WHERE unit_price > 190
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    product_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    AVG(unit_price_bucket_share) AS avg_unit_price_bucket_share,
    APPROX_PERCENTILE(unit_price_bucket, 0.5) AS unit_price_bucket_p50,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles
FROM reconciled
GROUP BY product_id;
