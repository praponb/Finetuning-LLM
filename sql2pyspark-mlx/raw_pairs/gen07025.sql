WITH filtered AS (
    SELECT
        id,
        customer_id,
        status,
        channel,
        amount,
        discount,
        FLOOR(shipping_fee) AS shipping_fee_whole,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-04-27'
      AND status IN ('PENDING', 'RETURNED', 'SHIPPED')
),
base AS (
    SELECT
        id,
        customer_id,
        channel,
        amount,
        discount,
        shipping_fee_whole,
        channel_clean,
        ROUND(shipping_fee_whole, 0) AS shipping_fee_whole_rounded,
        COALESCE(channel, 'NONE') AS channel_clean_2
    FROM filtered
    WHERE shipping_fee_whole <> 69
),
with_latest AS (
    SELECT
        s.*,
        latest.method,
        latest.paid_at
    FROM base s
    JOIN LATERAL (
            SELECT c.method, c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_latest s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.paid_at >= pr.starts_on
       AND s.paid_at <= DATE_ADD(pr.ends_on, 3)
)
SELECT
    id,
    customer_id,
    shipping_fee_whole,
    channel_clean,
    channel_clean_2,
    promo_id,
    DENSE_RANK() OVER w_recent AS shipping_fee_whole_dense_rank,
    COUNT(discount_pct) OVER (PARTITION BY promo_code ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS discount_pct_count_7d
FROM attributed
WINDOW w_recent AS (ORDER BY shipping_fee_whole DESC NULLS LAST)
