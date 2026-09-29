WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(order_date, 45) AS order_date_minus_45d,
        shipping_fee + discount AS shipping_fee_plus_discount,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM orders
    WHERE channel IS NOT DISTINCT FROM 'store'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.status = 'LOST'
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY customer_id, channel ORDER BY discount, id) AS discount_bucket,
        CUME_DIST() OVER w_hist AS discount_cume_dist
    FROM kept
    WINDOW w_hist AS (PARTITION BY status ORDER BY discount DESC NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        amount,
        discount,
        order_date_minus_45d,
        discount_bucket,
        discount_cume_dist,
        LEAD(discount_bucket) OVER w_part AS next_discount_bucket,
        LAG(amount) OVER w_part AS prev_amount,
        ROW_NUMBER() OVER w_part AS row_seq,
        AVG(discount) OVER w_grp AS discount_avg_6_prior
    FROM enriched
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY shipping_fee, id), w_grp AS (PARTITION BY MONTH(order_date), customer_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(discount_cume_dist) OVER w_running AS last7_max_discount_cume_dist,
        SUM(discount_cume_dist) OVER w_running AS last7_sum_discount_cume_dist,
        MIN(discount_cume_dist) OVER w_running AS last7_min_discount_cume_dist,
        COUNT(discount_cume_dist) OVER w_running AS last7_count_discount_cume_dist,
        SUM(discount_avg_6_prior) OVER w_recent AS running_discount_avg_6_prior
    FROM with_windows
    WHERE customer_id IN (6, 13, 24)
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY customer_id ORDER BY next_discount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.discount,
        s.discount_bucket,
        s.discount_cume_dist,
        s.prev_amount,
        s.last7_max_discount_cume_dist,
        s.last7_sum_discount_cume_dist,
        p.city,
        p.email,
        p.credit_limit
    FROM scored s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.discount,
        a.discount_bucket,
        a.last7_max_discount_cume_dist,
        a.last7_sum_discount_cume_dist,
        a.city,
        a.email,
        a.credit_limit,
        p.segment,
        p.age
    FROM with_dims a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN discount >= 0.44 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    AVG(last7_max_discount_cume_dist) AS avg_last7_max_discount_cume_dist,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MAX(last7_max_discount_cume_dist) FILTER (WHERE city = 'Bangkok') AS bangkok_max_last7_max_discount_cume_dist,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list_2,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY COUNT(*) FILTER (WHERE city = 'Chiang Mai') DESC) AS chiang_mai_rows_dense_rank
FROM joined
GROUP BY city, CASE WHEN discount >= 0.44 THEN 'HIGH' ELSE 'LOW' END
