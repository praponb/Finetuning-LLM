WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS item
),
lines AS (
    SELECT
        id,
        created_at,
        product_id,
        qty,
        element
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
with_windows AS (
    SELECT
        id,
        created_at,
        product_id,
        qty,
        element,
        MAX(qty) OVER (PARTITION BY YEAR(created_at), element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_max_1h,
        ROUND(qty / SUM(qty) OVER (PARTITION BY element), 4) AS qty_share,
        COUNT(qty) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_count_2h
    FROM lines
),
scored AS (
    SELECT
        *,
        MIN(qty_share) OVER (PARTITION BY element) AS element_min_qty_share,
        AVG(product_id) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_avg_15m,
        DENSE_RANK() OVER (PARTITION BY element ORDER BY qty_count_2h DESC) AS qty_count_2h_dense_rank,
        ROUND(qty_max_1h / SUM(qty_max_1h) OVER (PARTITION BY element), 4) AS qty_max_1h_share
    FROM with_windows
    WHERE created_at < '2025-06-19'
),
cleaned AS (
    SELECT *, product_id_avg_15m * qty_max_1h AS product_id_avg_15m_times_qty_max_1h
    FROM scored
    WHERE created_at >= DATE '2025-03-01'
      AND element IS NOT NULL
),
base AS (
    SELECT *, UPPER(element) AS element_upper
    FROM cleaned
    WHERE element IS DISTINCT FROM 'BUNDLE'
),
base_carts AS (
    SELECT *, WEEKOFYEAR(created_at) AS created_at_weekofyear
    FROM base
    WHERE created_at > TIMESTAMP '2025-03-02 00:00:00'
      AND qty_max_1h IS NOT NULL
)
SELECT
    product_id,
    qty,
    qty_max_1h,
    qty_share,
    element_min_qty_share,
    product_id_avg_15m,
    qty_count_2h_dense_rank,
    qty_max_1h_share,
    product_id_avg_15m_times_qty_max_1h,
    element_upper,
    created_at_weekofyear,
    STDDEV_SAMP(qty) OVER (PARTITION BY element) AS element_stddev_samp_qty
FROM base_carts
