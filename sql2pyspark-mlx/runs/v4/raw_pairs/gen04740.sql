WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LEAD(unit_price, 2) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS next_unit_price,
        ROW_NUMBER() OVER (ORDER BY unit_price ASC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id ASC) AS event_seq
    FROM order_items
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        next_unit_price,
        seq_no,
        event_seq,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY order_id), 4) AS event_seq_share,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY next_unit_price ASC NULLS FIRST) AS next_unit_price_dense_rank,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price
    FROM scored
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        next_unit_price,
        event_seq,
        event_seq_share,
        first_known_unit_price,
        CAST(next_unit_price AS INT) AS next_unit_price_int
    FROM metrics
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY event_seq_share ASC NULLS FIRST) AS event_seq_share_percent_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity ASC NULLS LAST) AS quantity_rank,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY next_unit_price_int ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM filtered
)
-- Running and moving aggregates
SELECT
    id,
    product_id,
    quantity,
    unit_price,
    next_unit_price,
    event_seq,
    event_seq_share,
    first_known_unit_price,
    next_unit_price_int,
    event_seq_share_percent_rank,
    quantity_rank,
    last_known_quantity,
    ROUND(next_unit_price / SUM(next_unit_price) OVER (PARTITION BY product_id), 4) AS next_unit_price_share,
    NTILE(10) OVER (PARTITION BY product_id ORDER BY first_known_unit_price ASC, id ASC) AS first_known_unit_price_bucket,
    ROUND(quantity_rank / SUM(quantity_rank) OVER (PARTITION BY product_id), 4) AS quantity_rank_share,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS seq_no
FROM with_windows;
