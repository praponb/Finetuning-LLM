SELECT
    id,
    city,
    segment,
    signup_date,
    RANK() OVER (PARTITION BY city, segment ORDER BY signup_date) signup_date_rank
FROM customers
ORDER BY signup_date ASC, signup_date_rank ASC;
