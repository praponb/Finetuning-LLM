WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IN (17, 33)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        unit_price / NULLIF(line_total, 0) AS unit_price_per_line_total,
        CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM cleaned
    WHERE quantity < 7
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(quantity, 1) OVER (ORDER BY line_total ASC, id ASC) AS next_quantity,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id, product_id) AS corr_unit_price_quantity,
        CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price
    FROM valid
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY corr_unit_price_quantity DESC, id ASC) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.channel,
        b.shipping_fee,
        b.status
    FROM ranked src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.product_name,
        p.stock_qty
    FROM joined e
    LEFT JOIN products p
        ON e.product_id = p.id
)
SELECT
    id,
    order_id,
    product_id,
    line_total,
    quantity_level,
    corr_unit_price_quantity,
    shipping_fee,
    status,
    product_name
FROM with_dims
ORDER BY corr_unit_price_quantity ASC;
