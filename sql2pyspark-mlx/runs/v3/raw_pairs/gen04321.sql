WITH base AS (
    SELECT *, FLOOR(shipping_fee) AS shipping_fee_whole
    FROM orders
    WHERE amount >= 1570 OR status IN ('RETURNED', 'SHIPPED', 'COMPLETED')
),
order_month_summary AS (
    SELECT MONTH(order_date) AS order_month, COUNT(*) AS total_count, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM base
    GROUP BY MONTH(order_date)
)
SELECT *
FROM order_month_summary
ORDER BY total_count DESC, order_month;
