WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS li_item
)
SELECT
    s.customer_id,
    s.coupon_codes,
    s.product_id,
    s.qty,
    s.price,
    s.promo,
    CASE WHEN promo RLIKE '^(BUNDLE|WELCOME5)$' THEN 'OTHER_ZONE' WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS promo_zone,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM lines s
ORDER BY product_id NULLS LAST;
