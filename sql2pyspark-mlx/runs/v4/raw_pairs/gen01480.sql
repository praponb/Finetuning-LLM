WITH base AS (
    SELECT *, CAST(quantity AS STRING) AS quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    SELECT s.*, o.amount
    FROM base s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
with_windows AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity NULLS LAST) AS quantity_cume_dist,
        LEAD(unit_price, 2) OVER (ORDER BY quantity, id) AS next_unit_price,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_cume_dist,
        LAG(quantity, 2, 0) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_quantity
    FROM with_dims
),
totals AS (
    SELECT
        order_id,
        product_id,
        BIT_OR(quantity) AS quantity_bit_or,
        BIT_OR(quantity) AS quantity_bit_or_2,
        COUNT(*) AS num_rows,
        APPROX_PERCENTILE(amount, 0.95) AS amount_p95
    FROM with_windows
    GROUP BY 1, 2
)
SELECT
    order_id,
    product_id,
    quantity_bit_or_2,
    num_rows,
    amount_p95
FROM totals
ORDER BY amount_p95 NULLS LAST, product_id DESC
