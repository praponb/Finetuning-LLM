WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity / unit_price, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.status,
        p.amount
    FROM filtered s
    JOIN orders p
        ON s.order_id = p.id
),
filtered_order_items AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(unit_price, line_total) AS least_unit_price_line_total,
        quantity + line_total AS quantity_plus_line_total
    FROM enriched
    WHERE status IS NOT NULL
)
SELECT
    id,
    quantity,
    unit_price,
    line_total,
    amount,
    least_unit_price_line_total
FROM filtered_order_items
