SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    AVG(discount) OVER (ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS discount_avg_2,
    AVG(shipping_fee) OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_3,
    CORR(shipping_fee, amount) OVER (PARTITION BY customer_id) AS corr_shipping_fee_amount,
    AVG(discount) OVER (PARTITION BY customer_id) AS customer_id_avg_discount,
    AVG(discount) OVER (PARTITION BY status, store_id ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_avg_3_prior
FROM orders
WHERE channel IN ('partner', 'mobile', 'store');
