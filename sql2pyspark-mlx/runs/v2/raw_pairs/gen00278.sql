WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE age >= 46 OR email LIKE '%@mail.co.th'
),
credit_limit_bucket_summary AS (
    SELECT
        CASE WHEN credit_limit >= 15900 THEN 'high' WHEN credit_limit >= 3000 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
        country,
        ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
        SUM(CASE WHEN country = 'TH' THEN credit_limit ELSE 0 END) AS th_credit_limit
    FROM filtered_customers
    GROUP BY CASE WHEN credit_limit >= 15900 THEN 'high' WHEN credit_limit >= 3000 THEN 'mid' ELSE 'low' END, country
)
SELECT *
FROM credit_limit_bucket_summary;
