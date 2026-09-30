WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        LEAD(amount, 2) OVER (PARTITION BY channel, customer_id ORDER BY amount, id) next_amount,
        NTILE(3) OVER w_grp shipping_fee_bucket
    FROM orders
    WHERE customer_id IN (6, 27, 30, 36)
    WINDOW w_grp AS (PARTITION BY status ORDER BY shipping_fee, id)
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, p.credit_limit
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_ADD(order_date, 46) order_date_plus_46d
    FROM with_dims
    WHERE channel IS NOT DISTINCT FROM 'mobile'
      AND shipping_fee_bucket = 4
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date_plus_46d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_shipping_fee,
        MIN(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date_plus_46d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date_plus_46d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_shipping_fee
    FROM valid
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        order_date,
        discount,
        shipping_fee,
        next_amount,
        credit_limit,
        order_date_plus_46d,
        running_min_shipping_fee,
        ROUND(shipping_fee_bucket / SUM(shipping_fee_bucket) OVER (PARTITION BY channel, customer_id), 4) shipping_fee_bucket_share,
        AVG(running_sum_shipping_fee) OVER (PARTITION BY store_id ORDER BY running_min_shipping_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) running_sum_shipping_fee_avg_6_prior,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY channel, store_id ORDER BY running_sum_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_shipping_fee
    FROM scored
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.delivered_date,
        x.shipped_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date, c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
report AS (
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.discount,
        s.shipping_fee,
        s.next_amount,
        s.shipping_fee_bucket_share,
        s.running_sum_shipping_fee_avg_6_prior,
        s.last_known_shipping_fee,
        (SELECT MAX(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_shipping_fee
    FROM latest_match s
)
SELECT
    id,
    order_date,
    discount,
    shipping_fee,
    next_amount,
    shipping_fee_bucket_share,
    running_sum_shipping_fee_avg_6_prior,
    last_known_shipping_fee,
    max_order_shipping_fee
FROM report
ORDER BY shipping_fee DESC NULLS FIRST;
