WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
metrics AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        DENSE_RANK() OVER (PARTITION BY customer_id, promo ORDER BY price) AS price_dense_rank,
        STDDEV_SAMP(product_id) OVER (PARTITION BY customer_id, promo) AS customer_id_stddev_samp_product_id,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY qty DESC) AS qty_percent_rank
    FROM exploded
),
scored AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        price_dense_rank,
        customer_id_stddev_samp_product_id,
        qty_percent_rank,
        COUNT(customer_id_stddev_samp_product_id) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS customer_id_stddev_samp_product_id_count_2h,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share
    FROM metrics
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.coupon_codes,
    s.qty,
    s.price,
    s.promo,
    s.price_dense_rank,
    s.customer_id_stddev_samp_product_id,
    CASE WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS promo_zone,
    (SELECT MAX(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS max_account_balance
FROM scored s;
