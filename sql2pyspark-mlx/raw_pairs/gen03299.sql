WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines_carts AS (
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        promo,
        element
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
scored AS (
    SELECT
        customer_id,
        product_id,
        qty,
        promo,
        element,
        CUME_DIST() OVER (PARTITION BY element ORDER BY product_id NULLS FIRST) product_id_cume_dist
    FROM lines_carts
),
joined AS (
    SELECT
        src.customer_id,
        src.product_id,
        src.qty,
        src.element,
        b.city
    FROM scored src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.age = 59
),
metrics AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_running product_id_percent_rank,
        ROUND(qty / SUM(qty) OVER (PARTITION BY city, customer_id), 4) qty_share
    FROM joined
    WINDOW w_running AS (PARTITION BY element, customer_id ORDER BY product_id DESC NULLS FIRST)
),
joined_carts AS (
    SELECT s.*, d.name
    FROM metrics s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY product_id_percent_rank NULLS FIRST) product_id_percent_rank_dense_rank,
        CUME_DIST() OVER (ORDER BY product_id_percent_rank DESC) product_id_percent_rank_cume_dist,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY qty NULLS LAST) qty_cume_dist,
        COUNT(qty) OVER (PARTITION BY city, customer_id) city_count_qty,
        RANK() OVER (ORDER BY qty_share DESC) qty_share_rank
    FROM joined_carts
)
SELECT
    product_id,
    qty,
    element,
    city,
    qty_share,
    name,
    product_id_percent_rank_dense_rank,
    product_id_percent_rank_cume_dist,
    qty_cume_dist
FROM with_windows
ORDER BY product_id_percent_rank_dense_rank NULLS FIRST, city DESC
