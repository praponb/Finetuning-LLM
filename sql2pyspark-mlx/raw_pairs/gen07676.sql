SELECT
    id,
    city,
    is_active,
    age,
    LEAD(age) OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS next_age,
    LAG(age) OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS prev_age,
    LEAD(credit_limit) OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS next_credit_limit,
    ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date), segment ORDER BY signup_date DESC, id) AS row_seq
FROM customers
