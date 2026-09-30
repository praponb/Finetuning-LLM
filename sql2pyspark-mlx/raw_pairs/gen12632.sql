WITH scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY MONTH(signup_date), segment ORDER BY age, id) AS age_bucket,
        NTILE(4) OVER (PARTITION BY city ORDER BY age, id) AS age_bucket_2,
        ROW_NUMBER() OVER (PARTITION BY YEAR(signup_date), country ORDER BY credit_limit, id) AS row_seq
    FROM customers
),
with_windows AS (
    SELECT
        *,
        MIN(credit_limit) OVER (PARTITION BY segment ORDER BY age_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_credit_limit,
        MAX(credit_limit) OVER (PARTITION BY segment ORDER BY age_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY segment ORDER BY age_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_credit_limit
    FROM scored
),
with_latest AS (
    SELECT s.*, latest.channel
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    SELECT *, LENGTH(channel) AS channel_length
    FROM with_latest
    WHERE is_active = TRUE
      AND signup_date >= DATE '2023-07-24'
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY age, id) AS seq_no,
    AVG(age_bucket) OVER (PARTITION BY YEAR(signup_date), country ORDER BY age_bucket_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS age_bucket_avg_5_prior,
    SUM(last7_max_credit_limit) OVER (PARTITION BY segment ORDER BY last7_sum_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_max_credit_limit
FROM valid
WHERE channel IN ('web', 'store')
