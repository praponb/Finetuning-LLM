WITH scored AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY unit_price NULLS LAST) AS unit_price_dense_rank
    FROM order_items
    WHERE product_id IS NOT NULL
),
base AS (
    SELECT *, GREATEST(seq_no, quantity) AS greatest_seq_no_quantity
    FROM scored
    WHERE product_id IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        SUM(greatest_seq_no_quantity) OVER (PARTITION BY order_id, product_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_seq_no_quantity,
        LEAD(quantity, 1, 0) OVER (ORDER BY seq_no, id) AS next_quantity
    FROM base
),
cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_dense_rank,
        running_greatest_seq_no_quantity,
        next_quantity,
        CAST(unit_price AS INT) AS unit_price_int
    FROM with_windows
    WHERE greatest_seq_no_quantity > 46
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    s.unit_price_dense_rank,
    s.running_greatest_seq_no_quantity,
    s.next_quantity,
    s.unit_price_int,
    (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS avg_order_item_unit_price
FROM cleaned s
ORDER BY next_quantity DESC NULLS FIRST;
