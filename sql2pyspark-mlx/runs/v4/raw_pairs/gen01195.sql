WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS elem
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        qty,
        price,
        EXPLODE(coupon_codes) AS tag
    FROM exploded
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        created_at,
        qty,
        price,
        tag,
        PERCENT_RANK() OVER (ORDER BY price DESC) AS price_percent_rank,
        MAX(qty) OVER w_ordered AS qty_max_30m
    FROM flattened
    WINDOW w_ordered AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(qty_max_30m, price) AS greatest_qty_max_30m_price,
        ROUND(price_percent_rank / price, 2) AS price_percent_rank_per_price
    FROM with_windows
    WHERE created_at >= DATE '2025-03-01'
      AND qty <= 3
)
SELECT
    id,
    customer_id,
    price,
    tag,
    price_percent_rank,
    greatest_qty_max_30m_price
FROM base
ORDER BY price_percent_rank NULLS LAST
