WITH enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        b.shipping_fee,
        b.amount
    FROM order_items src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY amount DESC, id ASC) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(quantity) OVER w_ordered AS quantity_avg_3_prior,
        SUM(quantity) OVER w_running AS running_quantity,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY product_id, order_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
        FIRST_VALUE(quantity) IGNORE NULLS OVER w_running AS first_known_quantity
    FROM latest
    WINDOW w_ordered AS (ORDER BY amount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_running AS (PARTITION BY order_id ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(first_known_quantity, TRUE) OVER (PARTITION BY product_id, order_id ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_quantity,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share
    FROM with_windows
    WHERE running_quantity = 15
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_quantity DESC NULLS FIRST, id ASC) = 1
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    dedup_rank,
    quantity_avg_3_prior,
    last_known_shipping_fee,
    first_known_quantity,
    first_known_first_known_quantity
FROM deduped
ORDER BY quantity_avg_3_prior ASC;
