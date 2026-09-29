SELECT
    id,
    customer_id,
    store_id,
    COUNT(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_shipping_fee,
    SUM(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_shipping_fee,
    AVG(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_shipping_fee,
    MAX(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_shipping_fee
FROM orders;
