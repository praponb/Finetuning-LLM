WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price / quantity, 2) AS line_total,
        ROUND(quantity * 100, 2) AS adjusted_quantity
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (14, 34)
),
base AS (
    SELECT
        *,
        quantity - line_total AS quantity_minus_line_total,
        CAST(line_total AS INT) AS line_total_int
    FROM cleaned
    WHERE product_id IS NOT NULL
),
scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY quantity_minus_line_total, id) AS quantity_minus_line_total_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total, id) AS event_seq,
        AVG(quantity_minus_line_total) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_minus_line_total_avg_3_prior,
        FIRST_VALUE(adjusted_quantity) IGNORE NULLS OVER (ORDER BY line_total_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjusted_quantity
    FROM base
),
joined AS (
    SELECT
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.line_total,
        s.line_total_int,
        s.event_seq,
        s.quantity_minus_line_total_avg_3_prior,
        p.category
    FROM scored s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_dims AS (
    SELECT
        e.id,
        e.product_id,
        e.line_total,
        e.line_total_int,
        e.event_seq,
        d.brand,
        d.stock_qty
    FROM joined e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE d.supplier_id IS NULL
)
SELECT
    s.id,
    s.product_id,
    s.line_total,
    s.event_seq,
    s.brand,
    CASE WHEN brand RLIKE '^(Acme|Stark)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS brand_zone,
    (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS max_inventory_quantity
FROM with_dims s
ORDER BY line_total;
