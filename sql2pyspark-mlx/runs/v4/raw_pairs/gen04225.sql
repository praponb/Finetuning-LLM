SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS seq_no,
    NTILE(3) OVER (PARTITION BY segment ORDER BY age, id) AS age_bucket,
    AVG(credit_limit) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_5_prior,
    SUM(credit_limit) OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
    STDDEV_SAMP(credit_limit) OVER (PARTITION BY segment, country) AS segment_stddev_samp_credit_limit
FROM customers
