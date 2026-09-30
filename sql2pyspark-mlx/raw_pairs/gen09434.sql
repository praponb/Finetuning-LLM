SELECT
    id,
    shipping_fee,
    discount,
    LAST_DAY(order_date) AS order_date_month_end,
    LOWER(status) AS status_lower
FROM orders
WHERE status = 'SHIPPED' OR status NOT IN ('RETURNED', 'PENDING')
ORDER BY discount DESC, 4, id
LIMIT 20;
