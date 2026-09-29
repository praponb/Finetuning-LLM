WITH matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM orders e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date >= sub.start_date
       AND e.order_date < sub.end_date + INTERVAL '7' DAY
),
matched_orders AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM matched e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
),
kept_keys AS (
    SELECT store_id, customer_id
    FROM matched_orders
    EXCEPT
    SELECT store_id, customer_id
    FROM orders
    WHERE status IN ('CANCELLED', 'PENDING')
),
kept_rows AS (
    SELECT s.*
    FROM matched_orders s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.customer_id = k.customer_id
),
scored AS (
    SELECT
        *,
        AVG(shipping_fee) OVER w_grp AS order_date_month_avg_shipping_fee,
        RANK() OVER (PARTITION BY store_id, plan ORDER BY subscription_id DESC NULLS FIRST) AS subscription_id_rank
    FROM kept_rows
    WHERE status IN ('RETURNED', 'CANCELLED')
    WINDOW w_grp AS (PARTITION BY MONTH(order_date), promo_code)
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.shipping_fee,
    s.subscription_id,
    s.monthly_fee,
    s.order_date_month_avg_shipping_fee,
    CASE WHEN status RLIKE '^(PENDING|COMPLETED)$' THEN 'EXTENDED' WHEN status RLIKE '^(CANCELLED|SHIPPED)$' THEN 'CORE' ELSE 'GROUP_A' END AS status_zone,
    (SELECT MAX(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_amount
FROM scored s;
