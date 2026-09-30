SELECT
    *,
    CORR(age, credit_limit) OVER (PARTITION BY segment) AS corr_age_credit_limit,
    ROW_NUMBER() OVER (ORDER BY age, id) AS row_seq
FROM customers
