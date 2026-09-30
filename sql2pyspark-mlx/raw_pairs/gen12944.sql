WITH kept_keys AS (
    -- Set difference on keys
    SELECT segment, country
    FROM customers
    EXCEPT
    SELECT segment, country
    FROM customers
    WHERE signup_date < '2024-10-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.country = k.country
)
-- Multi-level aggregation
SELECT
    segment,
    CASE WHEN age >= 64 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    CASE WHEN credit_limit >= 16400 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    SUM(CASE WHEN country = 'UK' THEN age ELSE 0 END) AS uk_age,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    APPROX_PERCENTILE(credit_limit, 0.5) AS credit_limit_p50,
    STDDEV_SAMP(age) AS age_stddev,
    RANK() OVER (PARTITION BY segment ORDER BY APPROX_PERCENTILE(credit_limit, 0.5) DESC) AS credit_limit_p50_rank
FROM reconciled
GROUP BY segment, CASE WHEN age >= 64 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN credit_limit >= 16400 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY age_stddev;
