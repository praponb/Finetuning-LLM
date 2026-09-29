WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        li_item.qty,
        li_item.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS li_item
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        qty,
        price,
        element
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        qty,
        ROUND(price / SUM(price) OVER (PARTITION BY element), 4) AS price_share,
        STDDEV_SAMP(price) OVER w_recent AS element_stddev_samp_price,
        RANK() OVER (PARTITION BY element ORDER BY price) AS price_rank,
        MIN(price) OVER w_recent AS element_min_price
    FROM lines
    WINDOW w_recent AS (PARTITION BY element)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(element_stddev_samp_price, 0) AS element_stddev_samp_price_filled
    FROM with_windows
    WHERE price_share > 0.69
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(qty) AS qty_whole
    FROM cleaned
    WHERE qty = 2
)
SELECT
    id,
    qty,
    price_share,
    element_stddev_samp_price,
    price_rank,
    element_min_price,
    element_stddev_samp_price_filled,
    qty_whole
FROM cleaned_carts
ORDER BY element_min_price DESC;
