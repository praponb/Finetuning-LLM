WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND YEAR(x.end_date) >= 2024
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM eligible
    EXCEPT ALL
    SELECT segment, id
    FROM customers
    WHERE age IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.segment = k.segment AND s.id = k.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date < s.signup_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(age, credit_limit) OVER (PARTITION BY country, segment) AS corr_age_credit_limit,
        CORR(credit_limit, age) OVER (PARTITION BY segment) AS corr_credit_limit_age,
        CUME_DIST() OVER (ORDER BY credit_limit DESC) AS credit_limit_cume_dist
    FROM not_flagged
),
scored_customers AS (
    -- Ranking and gap features
    SELECT *, AVG(corr_age_credit_limit) OVER (PARTITION BY city, segment) AS city_avg_corr_age_credit_limit
    FROM scored
    WHERE corr_credit_limit_age >= -0.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        country,
        corr_age_credit_limit,
        credit_limit_cume_dist
    FROM scored_customers
) src
UNPIVOT (
    value FOR measure IN (corr_age_credit_limit, credit_limit_cume_dist)
) unp;
