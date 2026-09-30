WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        base.id,
        base.created_at,
        base.coupon_codes,
        pos,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts base
    LATERAL VIEW POSEXPLODE(base.items) t AS pos, elem
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        pos,
        product_id,
        qty,
        price,
        value
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_ordered AS qty_rank,
        DENSE_RANK() OVER w_ordered AS qty_dense_rank
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY value ORDER BY qty DESC)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(qty_rank, 0) AS qty_rank_filled,
        CAST(pos AS DOUBLE) AS pos_str
    FROM scored
    WHERE price >= 80
)
-- Window metrics over several different partitions
SELECT
    product_id,
    qty,
    price,
    value,
    qty_rank,
    qty_dense_rank,
    qty_rank_filled,
    COUNT(qty_rank_filled) OVER (PARTITION BY value) AS value_count_qty_rank_filled,
    ROUND(qty / SUM(qty) OVER (PARTITION BY value), 4) AS qty_share
FROM filtered
