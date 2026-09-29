SELECT
    YEAR(order_date) order_year,
    DATE_TRUNC('MONTH', order_date) order_month,
    SUM(CASE WHEN channel = 'partner' THEN amount ELSE 0 END) partner_amount,
    COUNT(amount) amount_count
FROM orders
WHERE amount < 460
GROUP BY YEAR(order_date), DATE_TRUNC('MONTH', order_date)
