SELECT
    id,
    customer_id,
    end_date,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY end_date) AS end_date_pct_rank
FROM subscriptions;
