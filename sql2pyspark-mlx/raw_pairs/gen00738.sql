WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) x
),
scored AS (
    SELECT
        *,
        COUNT(qty) OVER (PARTITION BY customer_id) AS customer_id_count_qty,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY customer_id), 4) AS product_id_share
    FROM lines
),
metrics AS (
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        price,
        promo,
        product_id_share,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY price DESC) AS price_dense_rank,
        STDDEV_SAMP(customer_id_count_qty) OVER (PARTITION BY TO_DATE(created_at), promo) AS created_at_to_date_stddev_samp_customer_id_count_qty
    FROM scored
    WHERE customer_id_count_qty >= 10
),
enriched AS (
    SELECT
        s.customer_id,
        s.created_at,
        s.price,
        s.promo,
        s.product_id_share,
        s.price_dense_rank,
        s.created_at_to_date_stddev_samp_customer_id_count_qty,
        d.segment,
        d.country,
        d.signup_date
    FROM metrics s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
with_windows AS (
    SELECT
        created_at,
        price,
        promo,
        product_id_share,
        segment,
        country,
        COUNT(price_dense_rank) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS price_dense_rank_count_2h,
        MAX(price) OVER (PARTITION BY TO_DATE(created_at)) AS created_at_to_date_max_price,
        CUME_DIST() OVER (ORDER BY price DESC NULLS LAST) AS price_cume_dist
    FROM enriched
)
SELECT
    created_at,
    promo,
    product_id_share,
    segment,
    country,
    price_dense_rank_count_2h,
    price_cume_dist
FROM with_windows
ORDER BY segment DESC, price_dense_rank_count_2h DESC;
