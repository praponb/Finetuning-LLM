SELECT
    id,
    order_date,
    shipping_fee,
    customer_id,
    FIRST_VALUE(shipping_fee) OVER (ORDER BY order_date, id) AS first_shipping_fee,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) AS dense_rnk
FROM orders
WHERE discount > 0.11;
