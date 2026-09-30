WITH base AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled,
        ROUND(unit_price * 0.9, 2) AS adjusted_unit_price
    FROM order_items
    WHERE order_id IN (16, 28, 35)
),
scored AS (
    SELECT
        *,
        SUM(unit_price) OVER w_part AS cumulative_unit_price,
        LAST_VALUE(quantity) IGNORE NULLS OVER w_part AS last_known_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_filled, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS unit_price_bucket
    FROM base
    WINDOW w_part AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(quantity_filled, TRUE) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_filled,
        RANK() OVER (ORDER BY unit_price DESC) AS unit_price_rank,
        LAST_VALUE(unit_price, TRUE) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
    FROM scored
),
base_order_items AS (
    SELECT
        id,
        product_id,
        quantity,
        quantity_filled,
        cumulative_unit_price,
        event_seq,
        unit_price_bucket,
        last_known_quantity_filled,
        unit_price_rank,
        last_known_unit_price,
        ROUND(event_seq, 2) AS event_seq_rounded
    FROM metrics
    WHERE unit_price_bucket >= 4
),
enriched AS (
    SELECT
        *,
        FIRST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY event_seq DESC) AS event_seq_percent_rank,
        SUM(last_known_quantity_filled) OVER (PARTITION BY product_id ORDER BY last_known_quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_quantity_filled,
        NTILE(5) OVER (ORDER BY unit_price_rank, id) AS unit_price_rank_bucket,
        ROUND(unit_price_rank / SUM(unit_price_rank) OVER (PARTITION BY product_id), 4) AS unit_price_rank_share
    FROM base_order_items
)
SELECT
    id,
    quantity,
    cumulative_unit_price,
    event_seq,
    last_known_unit_price,
    event_seq_rounded,
    first_known_event_seq,
    event_seq_percent_rank,
    running_last_known_quantity_filled
FROM enriched
ORDER BY event_seq;
