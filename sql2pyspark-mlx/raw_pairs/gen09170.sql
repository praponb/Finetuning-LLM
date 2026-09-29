SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    amount,
    discount,
    shipping_fee,
    MIN(amount) OVER w_hist AS last3_min_amount,
    AVG(amount) OVER w_hist AS last3_avg_amount,
    SUM(amount) OVER w_hist AS last3_sum_amount,
    MAX(amount) OVER w_hist AS last3_max_amount,
    AVG(discount) OVER (PARTITION BY channel ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6
FROM orders
WINDOW w_hist AS (PARTITION BY store_id ORDER BY discount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
