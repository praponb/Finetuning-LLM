WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price DESC, id) AS rn
        FROM order_items
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rn DESC, id) AS dedup_rank
        FROM latest
    ) t
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT src.*, d.brand
    FROM ranked src
    INNER JOIN products d
        ON src.product_id = d.id
)
SELECT
    id,
    order_id,
    quantity,
    unit_price,
    rn,
    dedup_rank,
    brand
FROM joined
ORDER BY quantity
