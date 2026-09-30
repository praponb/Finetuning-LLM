SELECT
    id,
    city,
    signup_date,
    segment,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS row_num,
    PERCENT_RANK() OVER (PARTITION BY segment ORDER BY signup_date) AS signup_date_pct_rank
FROM customers;
