WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        amount,
        discount,
        shipping_fee,
        COUNT(shipping_fee) OVER w_running AS running_count_shipping_fee,
        MIN(shipping_fee) OVER w_running AS running_min_shipping_fee,
        AVG(shipping_fee) OVER w_running AS running_avg_shipping_fee
    FROM orders
    WINDOW w_running AS (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    customer_id,
    running_count_shipping_fee,
    running_min_shipping_fee,
    running_avg_shipping_fee
FROM with_windows
ORDER BY running_min_shipping_fee DESC NULLS LAST;
