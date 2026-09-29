WITH cleaned AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity AS line_total
    FROM order_items
    WHERE quantity >= 6
),
joined AS (
    SELECT
        src.*,
        b.product_name,
        b.cost
    FROM cleaned src
    LEFT JOIN products b
        ON src.product_id = b.id
),
filtered AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        product_name,
        quantity * unit_price AS line_total_2
    FROM joined
    WHERE product_id IS NOT NULL
      AND line_total BETWEEN 23 AND 64
),
joined_order_items AS (
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        e.unit_price,
        p.category,
        p.stock_qty
    FROM filtered e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.stock_qty > 213
),
enriched AS (
    SELECT
        e.*,
        p.price,
        p.product_name
    FROM joined_order_items e
    LEFT JOIN products p
        ON e.product_id <=> p.id
),
latest AS (
    SELECT
        id,
        quantity,
        product_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
cleaned_order_items AS (
    SELECT *, REPLACE(product_name, ' ', '_') AS product_name_slug
    FROM latest
    WHERE quantity != 3
)
SELECT
    id,
    quantity,
    product_name
FROM cleaned_order_items
ORDER BY quantity
