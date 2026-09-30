WITH cleaned AS (
    SELECT *, ROUND(quantity * 100, 2) quantity_adjusted
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        product_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) row_seq,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) unit_price_percent_rank
    FROM cleaned
),
joined AS (
    SELECT
        src.id,
        src.product_id,
        src.row_seq,
        b.stock_qty,
        b.launch_date
    FROM with_windows src
    LEFT JOIN products b
        ON src.product_id = b.id
),
ranked AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY launch_date DESC, id) = 1
),
with_dims AS (
    SELECT
        s.*,
        p.product_name,
        p.brand,
        p.category
    FROM ranked s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_dims_order_items AS (
    SELECT
        s.*,
        p.brand AS product_brand,
        p.price
    FROM with_dims s
    INNER JOIN products p
        ON s.product_id <=> p.id
)
SELECT
    id,
    row_seq,
    launch_date,
    brand,
    product_brand,
    price
FROM with_dims_order_items;
