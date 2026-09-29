SELECT
    id,
    city,
    signup_date,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY signup_date) AS signup_date_pct_rank
FROM customers;
