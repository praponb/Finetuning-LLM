WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity / unit_price, 2) AS line_total,
        CEIL(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (1, 8, 17, 29)
),
with_dims AS (
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.line_total,
        d.amount,
        d.order_date,
        d.discount
    FROM cleaned e
    INNER JOIN orders d
        ON e.order_id = d.id
),
metrics AS (
    SELECT
        *,
        AVG(line_total) OVER (PARTITION BY product_id) AS product_id_avg_line_total,
        SUM(discount) OVER (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        PERCENT_RANK() OVER w_running AS discount_percent_rank,
        LEAD(line_total, 2) OVER (PARTITION BY order_id ORDER BY line_total, id) AS next_line_total
    FROM with_dims
    WHERE discount BETWEEN 0.12 AND 0.4
    WINDOW w_running AS (PARTITION BY YEAR(order_date), order_id ORDER BY discount DESC NULLS FIRST)
),
metrics_order_items AS (
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY product_id, order_id ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', order_date), product_id ORDER BY discount DESC, id) AS discount_bucket
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY product_id_avg_line_total, id)
),
base AS (
    SELECT
        id,
        order_id,
        line_total,
        amount,
        discount,
        product_id_avg_line_total,
        running_discount,
        next_line_total,
        discount_avg_6,
        row_seq,
        discount_bucket,
        DATE_ADD(order_date, 43) AS order_date_plus_43d
    FROM metrics_order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        line_total,
        amount,
        product_id_avg_line_total,
        next_line_total,
        discount_avg_6,
        discount_bucket,
        AVG(next_line_total) OVER w_ordered AS next_line_total_avg_6,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY next_line_total, id) AS row_seq_2,
        FIRST_VALUE(discount_avg_6, TRUE) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_avg_6,
        SUM(discount_avg_6) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_avg_6,
        AVG(running_discount) OVER w_running AS running_discount_avg_6_prior
    FROM base
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY product_id_avg_line_total, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id ORDER BY discount_avg_6, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    line_total,
    amount,
    product_id_avg_line_total,
    next_line_total,
    discount_avg_6,
    discount_bucket,
    next_line_total_avg_6,
    row_seq_2
FROM enriched;
