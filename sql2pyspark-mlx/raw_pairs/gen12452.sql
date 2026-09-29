WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        HOUR(paid_at) AS paid_at_hour,
        CASE WHEN amount >= 1410 THEN 'High' WHEN amount >= 710 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM payments
    WHERE paid_at > DATE '2023-10-01'
      AND amount <= 1560
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.shipping_fee,
        p.order_date,
        p.channel
    FROM filtered src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.paid_at,
        a.paid_at_hour,
        a.shipping_fee,
        a.channel,
        o.shipping_fee AS order_shipping_fee
    FROM enriched a
    JOIN orders o
        ON a.order_id <=> o.id
    WHERE o.customer_id = 15
),
kept_keys AS (
    -- Set difference on keys
    SELECT method, order_id
    FROM joined
    EXCEPT
    SELECT method, order_id
    FROM payments
    WHERE TO_DATE(paid_at) < '2024-06-10'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN kept_keys k ON s.method = k.method AND s.order_id = k.order_id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept_rows s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.paid_at >= pr.starts_on
       AND s.paid_at < pr.ends_on + INTERVAL 14 DAYS
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        paid_at_hour,
        shipping_fee,
        order_shipping_fee,
        promo_id,
        promo_code,
        SUM(shipping_fee) OVER (PARTITION BY DATE_TRUNC('YEAR', paid_at), order_id) AS paid_at_period_sum_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY TO_DATE(paid_at), order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS shipping_fee_sum_30d
    FROM attributed
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        order_id,
        order_shipping_fee,
        shipping_fee
    FROM metrics
) src
UNPIVOT (
    value FOR metric_name IN (order_shipping_fee, shipping_fee)
);
