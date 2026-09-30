WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT account_type, status
    FROM accounts
    INTERSECT
    SELECT account_type, status
    FROM accounts
    WHERE opened_date >= '2024-06-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    JOIN remaining_keys k ON s.account_type = k.account_type AND s.status = k.status
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        d.credit_limit,
        d.signup_date,
        d.age
    FROM reconciled src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        signup_date,
        age,
        ROUND(age * 1.15, 2) AS adjusted_age,
        age * credit_limit AS age_times_credit_limit
    FROM enriched
    WHERE customer_id IS NULL
),
scored AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY age DESC NULLS LAST) AS age_dense_rank
    FROM valid
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    APPROX_PERCENTILE(age_times_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS age_times_credit_limit_quartiles,
    APPROX_PERCENTILE(age_times_credit_limit, 0.9) AS age_times_credit_limit_p90,
    AVG(age_times_credit_limit) AS avg_age_times_credit_limit
FROM scored
GROUP BY DATE_TRUNC('QUARTER', signup_date)
HAVING APPROX_PERCENTILE(age_times_credit_limit, 0.9) > 0;
