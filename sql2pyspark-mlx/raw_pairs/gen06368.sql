WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        CONCAT(city, '/', name) city_name
    FROM customers
    WHERE city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.signup_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT segment, city
    FROM kept
    EXCEPT DISTINCT
    SELECT segment, city
    FROM customers
    WHERE LENGTH(name) >= 6
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.segment = k.segment AND s.city = k.city
),
kept_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.start_date < s.signup_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        city_name,
        DENSE_RANK() OVER (ORDER BY age DESC NULLS LAST) age_dense_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), segment), 4) credit_limit_share,
        CUME_DIST() OVER (ORDER BY credit_limit) credit_limit_cume_dist
    FROM kept_customers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        city,
        segment,
        signup_date,
        city_name,
        age_dense_rank,
        AVG(credit_limit_cume_dist) OVER (PARTITION BY city) city_avg_credit_limit_cume_dist,
        CORR(credit_limit, age) OVER (PARTITION BY city) corr_credit_limit_age,
        MIN(age_dense_rank) OVER (PARTITION BY MONTH(signup_date), segment) signup_date_month_min_age_dense_rank
    FROM scored
    WHERE credit_limit_cume_dist < 0.28
)
SELECT
    city,
    segment,
    signup_date,
    city_name,
    age_dense_rank,
    city_avg_credit_limit_cume_dist,
    corr_credit_limit_age,
    signup_date_month_min_age_dense_rank
FROM enriched
ORDER BY signup_date DESC;
