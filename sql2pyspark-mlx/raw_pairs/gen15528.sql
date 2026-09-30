WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity, id) quantity_bucket,
        RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS LAST) quantity_rank
    FROM order_items
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(quantity_rank, quantity) greatest_quantity_rank_quantity,
        ROUND(quantity, 2) quantity_rounded
    FROM with_windows
    WHERE order_id = 4
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        unit_price,
        quantity_bucket,
        quantity_rank,
        greatest_quantity_rank_quantity,
        LEAD(quantity) OVER (PARTITION BY order_id ORDER BY quantity_bucket, id) next_quantity,
        LAG(quantity) OVER (PARTITION BY order_id ORDER BY quantity_bucket, id) prev_quantity,
        LAG(greatest_quantity_rank_quantity) OVER (PARTITION BY order_id ORDER BY quantity_bucket, id) prev_greatest_quantity_rank_quantity,
        LEAD(greatest_quantity_rank_quantity) OVER (PARTITION BY order_id ORDER BY quantity_bucket, id) next_greatest_quantity_rank_quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_bucket, id) seq_no,
        ROUND(quantity_bucket / SUM(quantity_bucket) OVER (PARTITION BY order_id), 4) quantity_bucket_share
    FROM base
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.quantity_bucket,
        src.next_greatest_quantity_rank_quantity,
        b.amount
    FROM scored src
    INNER JOIN orders b
        ON src.order_id = b.id
    WHERE b.amount > 620
),
report AS (
    SELECT
        id,
        next_greatest_quantity_rank_quantity,
        amount
    FROM enriched
    ORDER BY next_greatest_quantity_rank_quantity DESC NULLS LAST, id DESC
    LIMIT 10
)
SELECT id, amount
FROM report
ORDER BY amount DESC;
