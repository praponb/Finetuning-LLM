WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS item
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, PERCENT_RANK() OVER (ORDER BY qty ASC) AS qty_percent_rank
    FROM exploded
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        price,
        qty_percent_rank,
        CASE WHEN product_id >= 10 THEN 'High' WHEN product_id >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_level
    FROM scored
    WHERE qty_percent_rank > 0.21
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN price >= 240 THEN 'High' WHEN price >= 63 THEN 'Medium' ELSE 'Low' END AS price_level
    FROM cleaned
    WHERE qty_percent_rank > 0.61
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(price AS INT) AS price_int
    FROM cleaned_carts
    WHERE qty_percent_rank >= 0.76
)
SELECT
    id,
    qty_percent_rank,
    product_id_level,
    price_level
FROM base;
