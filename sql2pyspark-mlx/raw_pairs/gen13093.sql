WITH remaining_keys AS (
    -- Set difference on keys
    SELECT city
    FROM customers
    EXCEPT
    SELECT city
    FROM customers
    WHERE UPPER(TRIM(email)) = 'GRACE.KIM@GMAIL.COM'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN remaining_keys k ON s.city = k.city
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, CORR(age, credit_limit) OVER (PARTITION BY city) corr_age_credit_limit
    FROM reconciled
    WHERE city IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.browser = 'Safari'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        age,
        DENSE_RANK() OVER w_part age_dense_rank,
        CORR(credit_limit, age) OVER w_hist corr_credit_limit_age,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('month', signup_date), country ORDER BY corr_age_credit_limit DESC) corr_age_credit_limit_dense_rank
    FROM kept
    WINDOW w_part AS (PARTITION BY city, segment ORDER BY age DESC NULLS LAST), w_hist AS (PARTITION BY city)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    COUNT_IF(city = 'Tokyo') AS tokyo_events,
    PERCENTILE_APPROX(corr_credit_limit_age, 0.95) AS corr_credit_limit_age_p95
FROM metrics
GROUP BY DATE_TRUNC('MONTH', signup_date)
ORDER BY tokyo_events DESC;
