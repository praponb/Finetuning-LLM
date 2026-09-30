SELECT
    *,
    NTILE(4) OVER w_part AS credit_limit_bucket,
    CORR(age, credit_limit) OVER (PARTITION BY segment, city) AS corr_age_credit_limit,
    ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY signup_date DESC, id) AS seq_no
FROM customers
WINDOW w_part AS (ORDER BY credit_limit, id)
