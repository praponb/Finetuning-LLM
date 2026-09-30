SELECT
    id,
    customer_id,
    order_date,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_date_pct_rank
FROM orders;
