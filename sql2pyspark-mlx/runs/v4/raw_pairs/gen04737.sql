WITH attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.promo_id,
    s.promo_code,
    s.discount_pct,
    CASE WHEN promo_code RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS promo_code_zone,
    (SELECT SUM(t2.shipping_fee) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS sum_order_shipping_fee
FROM attributed s
ORDER BY promo_code DESC NULLS LAST, promo_id DESC;
