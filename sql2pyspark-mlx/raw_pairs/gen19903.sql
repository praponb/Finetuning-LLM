WITH lines AS (
    SELECT
        c.id,
        c.customer_id,
        c.coupon_codes,
        elem.price,
        elem.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) e AS elem
),
flattened AS (
    SELECT
        id,
        customer_id,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM lines
),
enriched AS (
    SELECT
        a.*,
        c.segment,
        c.country,
        c.signup_date
    FROM flattened a
    INNER JOIN customers c
        ON a.customer_id <=> c.id
),
metrics AS (
    SELECT
        price,
        promo,
        value,
        segment,
        DENSE_RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_dense_rank,
        PERCENT_RANK() OVER (ORDER BY price DESC) AS price_percent_rank,
        DENSE_RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_dense_rank_2
    FROM enriched
    WHERE promo IN ('SPRING10', 'BUNDLE', 'VIP')
),
valid AS (
    SELECT
        promo,
        value,
        segment,
        price_dense_rank,
        price_percent_rank,
        price_dense_rank_2,
        UPPER(promo) AS promo_upper,
        price_dense_rank / NULLIF(price_percent_rank, 0) AS price_dense_rank_per_price_percent_rank,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM metrics
    WHERE value = 'WELCOME5'
),
metrics_carts AS (
    SELECT
        *,
        COUNT(price_percent_rank) OVER (PARTITION BY value, promo) AS value_count_price_percent_rank,
        MAX(price_dense_rank_per_price_percent_rank) OVER (PARTITION BY segment) AS segment_max_price_dense_rank_per_price_percent_rank,
        MAX(price_percent_rank) OVER (PARTITION BY promo, segment) AS promo_max_price_percent_rank,
        COUNT(price_dense_rank_per_price_percent_rank) OVER (PARTITION BY segment) AS segment_count_price_dense_rank_per_price_percent_rank,
        SUM(price_dense_rank_2) OVER (PARTITION BY value) AS value_sum_price_dense_rank_2
    FROM valid
),
with_windows AS (
    SELECT
        *,
        MAX(value_sum_price_dense_rank_2) OVER (PARTITION BY value) AS value_max_value_sum_price_dense_rank_2,
        MIN(promo_max_price_percent_rank) OVER (PARTITION BY promo) AS promo_min_promo_max_price_percent_rank
    FROM metrics_carts
)
SELECT
    value,
    segment,
    price_dense_rank,
    promo_upper,
    price_dense_rank_per_price_percent_rank,
    resolved_promo,
    promo_max_price_percent_rank,
    segment_count_price_dense_rank_per_price_percent_rank,
    value_sum_price_dense_rank_2
FROM with_windows
ORDER BY price_dense_rank DESC NULLS LAST, price_dense_rank_per_price_percent_rank DESC;
