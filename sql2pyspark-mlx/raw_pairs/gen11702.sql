WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS(',', country, email) AS country_email_key,
        UPPER(email) AS email_upper
    FROM customers
    WHERE signup_date > DATE '2023-04-11'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        country_email_key,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        NTILE(3) OVER (PARTITION BY country ORDER BY credit_limit, id) AS credit_limit_bucket,
        AVG(credit_limit) OVER (PARTITION BY MONTH(signup_date), city ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_3_prior
    FROM cleaned
    WHERE MONTH(signup_date) IN (5, 6, 7)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        country_email_key,
        credit_limit_bucket,
        credit_limit_avg_3_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit_avg_3_prior NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.shipping_fee > 12
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit_avg_3_prior >= 17980 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_avg_3_prior_tier,
    PERCENTILE_APPROX(credit_limit_avg_3_prior, 0.95) AS credit_limit_avg_3_prior_p95,
    COUNT(*) AS num_rows,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(credit_limit_bucket, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_bucket_quartiles
FROM eligible
GROUP BY credit_limit_avg_3_prior_tier
ORDER BY credit_limit_avg_3_prior_p95 DESC
