WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), order_date) / 365 AS years_since_order_date,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM orders
    WHERE order_date >= DATE '2023-09-14'
      AND shipping_fee < 15
),
kept_keys AS (
    SELECT store_id
    FROM cleaned
    INTERSECT
    SELECT store_id
    FROM orders
    WHERE channel <> 'store'
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.store_id = k.store_id
),
attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM verified e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date < pr.ends_on + INTERVAL '7' DAY
),
attributed_orders AS (
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM attributed s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
),
base AS (
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        discount,
        years_since_order_date,
        promo_id,
        promo_code,
        discount_pct,
        promo_id_2,
        promotion_promo_code,
        promotion_discount_pct,
        SUBSTRING(promo_code, 1, 3) AS promo_code_prefix,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM attributed_orders
    WHERE order_date > DATE '2023-05-17'
      AND customer_id IS NOT NULL
),
filtered AS (
    SELECT
        customer_id,
        store_id,
        status,
        amount,
        discount,
        years_since_order_date,
        promo_id,
        promo_code,
        promotion_promo_code,
        CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END AS is_pending
    FROM base
    WHERE promo_code IN ('SPRING10', 'VIP')
),
base_orders AS (
    SELECT
        *,
        ROUND(amount, 0) AS amount_rounded,
        TRIM(status) AS clean_status,
        COALESCE(promo_code, 'DIRECT') AS resolved_promo_code
    FROM filtered
    WHERE promotion_promo_code IS DISTINCT FROM 'BUNDLE'
)
SELECT *
FROM (
    SELECT
        promo_code,
        amount,
        amount_rounded,
        discount,
        years_since_order_date
    FROM base_orders
) src
UNPIVOT (
    val FOR metric_name IN (amount, amount_rounded AS AMOUNT_ROUNDED_VAL, discount AS DISCOUNT, years_since_order_date AS YEARS_SINCE_ORDER_DATE)
);
