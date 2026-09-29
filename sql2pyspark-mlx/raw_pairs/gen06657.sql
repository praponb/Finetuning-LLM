WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price ASC NULLS FIRST, id ASC) AS row_num
        FROM order_items
    ) x
    WHERE row_num = 1
),
with_dims AS (
    SELECT e.*, d.channel
    FROM deduped e
    INNER JOIN orders d
        ON e.order_id = d.id
),
deduped_order_items AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id ASC) AS dedup_rank
        FROM with_dims
    ) x
    WHERE dedup_rank = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC) AS rank_in_group
        FROM deduped_order_items
    ) t
    WHERE rank_in_group = 1
)
SELECT
    p.brand,
    s.channel,
    COUNT(*) AS rows_total,
    MAX(s.quantity) AS max_quantity
FROM latest s
LEFT JOIN products p ON s.product_id = p.id
GROUP BY p.brand, s.channel
