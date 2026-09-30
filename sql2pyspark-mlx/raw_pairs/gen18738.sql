SELECT
    *,
    SUM(discount) OVER w_running AS running_sum_discount,
    COUNT(discount) OVER w_running AS running_count_discount,
    AVG(discount) OVER w_running AS running_avg_discount,
    PERCENT_RANK() OVER (PARTITION BY YEAR(order_date), customer_id ORDER BY discount) AS discount_percent_rank
FROM orders
WINDOW w_running AS (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
