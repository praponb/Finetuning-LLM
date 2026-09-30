WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(email, 1, 2) AS email_prefix,
        COALESCE(segment, 'UNKNOWN') AS segment_clean
    FROM customers
    WHERE signup_date >= DATE '2023-04-18'
      AND NOT is_active
),
with_windows AS (
    SELECT
        id,
        email,
        signup_date,
        age,
        email_prefix,
        segment_clean,
        NTILE(10) OVER w_grp AS credit_limit_bucket,
        NTILE(5) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS credit_limit_bucket_2
    FROM cleaned
    WHERE country != 'US'
    WINDOW w_grp AS (PARTITION BY YEAR(signup_date) ORDER BY credit_limit DESC, id)
),
kept_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT
    SELECT id
    FROM customers
    WHERE NOT is_active
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_windows_customers AS (
    SELECT
        email,
        signup_date,
        age,
        segment_clean,
        credit_limit_bucket,
        PERCENT_RANK() OVER (ORDER BY credit_limit_bucket_2 NULLS FIRST) AS credit_limit_bucket_2_percent_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(signup_date) ORDER BY age DESC) AS age_dense_rank,
        CUME_DIST() OVER (ORDER BY credit_limit_bucket_2 NULLS LAST) AS credit_limit_bucket_2_cume_dist
    FROM verified
),
filtered AS (
    SELECT
        email,
        signup_date,
        age,
        segment_clean,
        credit_limit_bucket,
        credit_limit_bucket_2_percent_rank,
        age_dense_rank,
        credit_limit_bucket_2_cume_dist,
        age_dense_rank + age AS age_dense_rank_plus_age
    FROM with_windows_customers
    WHERE signup_date >= DATE '2023-05-18'
),
cleaned_customers AS (
    SELECT *, GREATEST(age_dense_rank, age) AS greatest_age_dense_rank_age
    FROM filtered
    WHERE signup_date > DATE '2023-07-31'
)
SELECT
    CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    CASE WHEN credit_limit_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_bucket_tier,
    MIN(age) AS min_age,
    ROUND(AVG(credit_limit_bucket_2_percent_rank), 2) AS avg_credit_limit_bucket_2_percent_rank,
    APPROX_PERCENTILE(credit_limit_bucket, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_bucket_quartiles,
    AVG(age) AS avg_age,
    APPROX_PERCENTILE(age_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS age_dense_rank_quartiles
FROM cleaned_customers
GROUP BY CASE WHEN age >= 58 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN credit_limit_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END;
