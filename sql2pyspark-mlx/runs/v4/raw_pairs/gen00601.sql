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
    NTILE(5) OVER w_hist AS amount_bucket,
    SUM(amount) OVER w_grp AS running_amount,
    NTILE(4) OVER w_recent AS discount_bucket
FROM orders
WINDOW w_hist AS (PARTITION BY customer_id ORDER BY amount ASC, id ASC), w_grp AS (PARTITION BY status ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY store_id ORDER BY discount ASC, id ASC);
