WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        discount,
        INITCAP(TRIM(channel)) channel_title,
        LOWER(channel) channel_lower
    FROM orders
    WHERE order_date > TO_DATE('2023-06-08')
      AND store_id IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        customer_id,
        status,
        channel,
        amount,
        MIN(discount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_discount,
        AVG(discount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_discount,
        MAX(discount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_discount,
        COUNT(discount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_discount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id, channel), 4) amount_share
    FROM cleaned
),
joined AS (
    SELECT
        src.*,
        p.age,
        p.credit_limit,
        p.city
    FROM with_windows src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
with_dims AS (
    SELECT
        a.*,
        p.email,
        p.age AS customer_age,
        p.city AS customer_city
    FROM joined a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
not_flagged AS (
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id = 36
        )
),
with_latest AS (
    SELECT
        s.id,
        s.status,
        s.channel,
        s.amount,
        s.running_min_discount,
        s.running_avg_discount,
        s.running_max_discount,
        s.running_count_discount,
        s.age,
        s.credit_limit,
        s.city,
        s.email,
        top1.order_item_unit_price
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        id,
        status,
        channel,
        amount,
        running_max_discount,
        running_count_discount,
        age,
        credit_limit,
        city,
        STDDEV_SAMP(age) OVER (PARTITION BY city) city_stddev_samp_age,
        LAST_VALUE(age) IGNORE NULLS OVER (PARTITION BY channel ORDER BY running_max_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_age
    FROM with_latest
    WHERE credit_limit >= 5000
)
SELECT
    id,
    status,
    channel,
    running_max_discount,
    running_count_discount,
    age,
    city,
    city_stddev_samp_age,
    NTILE(4) OVER (ORDER BY running_count_discount DESC, id) running_count_discount_bucket,
    SUM(running_max_discount) OVER (PARTITION BY city, status ORDER BY running_count_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_running_max_discount
FROM enriched;
