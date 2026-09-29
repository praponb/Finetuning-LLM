WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        CASE WHEN unit_price >= 340 THEN 'High' WHEN unit_price >= 160 THEN 'Medium' ELSE 'Low' END AS unit_price_band,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (18, 34)
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, b.amount
    FROM base a
    INNER JOIN orders b
        ON a.order_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price_band,
        line_total,
        amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total DESC, id) AS row_seq,
        STDDEV_SAMP(line_total) OVER (PARTITION BY order_id) AS order_id_stddev_samp_line_total,
        FIRST_VALUE(line_total, TRUE) OVER (PARTITION BY order_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_line_total,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total, id) AS row_seq_2
    FROM joined
)
SELECT
    id,
    line_total,
    order_id_stddev_samp_line_total,
    first_known_line_total
FROM scored;
