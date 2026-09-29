WITH lines AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        li_item.product_id,
        li_item.price,
        li_item.promo
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) t AS li_item
),
joined AS (
    SELECT
        e.customer_id,
        e.created_at,
        e.promo,
        d.name,
        d.age,
        d.segment
    FROM lines e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
metrics AS (
    SELECT
        customer_id,
        created_at,
        CUME_DIST() OVER (ORDER BY age DESC NULLS FIRST) AS age_cume_dist,
        ROUND(age / SUM(age) OVER (PARTITION BY promo), 4) AS age_share,
        COUNT(age) OVER (PARTITION BY TO_DATE(created_at), segment) AS created_at_to_date_count_age,
        SUM(age) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS age_sum_2h,
        ROUND(age / SUM(age) OVER (PARTITION BY promo), 4) AS age_share_2
    FROM joined
    WHERE name IS NOT NULL
),
base AS (
    SELECT *, age_cume_dist * created_at_to_date_count_age AS age_cume_dist_times_created_at_to_date_count_age
    FROM metrics
    WHERE customer_id IS NOT NULL
),
joined_carts AS (
    SELECT s.*, b.signup_date
    FROM base s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    s.customer_id,
    s.created_at,
    s.age_cume_dist,
    s.age_share,
    s.created_at_to_date_count_age,
    s.age_share_2,
    (SELECT MIN(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_shipping_fee
FROM joined_carts s
ORDER BY customer_id ASC;
