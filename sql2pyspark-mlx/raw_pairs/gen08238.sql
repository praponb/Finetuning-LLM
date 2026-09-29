WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        channel,
        shipping_fee,
        LAG(discount, 1) OVER (ORDER BY order_date, id) AS prev_discount,
        AVG(discount) OVER (PARTITION BY customer_id, status ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_avg_3,
        SUM(amount) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM orders
    WHERE discount BETWEEN 0.1 AND 0.34
)
SELECT
    id,
    shipping_fee,
    prev_discount,
    discount_avg_3,
    cumulative_amount
FROM metrics
ORDER BY shipping_fee NULLS LAST, id
