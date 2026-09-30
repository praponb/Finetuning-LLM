WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        quantity + unit_price AS line_total,
        CEIL(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.line_total,
        p.shipping_fee
    FROM cleaned src
    LEFT JOIN orders p
        ON src.order_id = p.id
    WHERE YEAR(p.order_date) >= 2023
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        DENSE_RANK() OVER w_recent AS shipping_fee_dense_rank,
        AVG(line_total) OVER (PARTITION BY order_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS line_total_avg_6
    FROM joined
    WINDOW w_recent AS (ORDER BY shipping_fee NULLS LAST)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total DESC NULLS LAST, id) AS rn
    FROM with_windows
    QUALIFY rn = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY shipping_fee_dense_rank DESC NULLS LAST) AS dedup_rank
        FROM latest
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    id,
    order_id,
    line_total,
    shipping_fee,
    shipping_fee_bucket,
    shipping_fee_dense_rank,
    line_total_avg_6,
    rn
FROM deduped;
