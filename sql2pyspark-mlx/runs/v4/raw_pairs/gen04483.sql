WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        NTILE(10) OVER w_recent AS age_bucket,
        MIN(age) OVER (PARTITION BY country) AS country_min_age
    FROM customers
    WINDOW w_recent AS (ORDER BY age, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.country = 'US'
        )
)
SELECT
    id,
    email,
    country,
    signup_date,
    is_active,
    credit_limit,
    age,
    age_bucket,
    country_min_age,
    CASE WHEN country RLIKE '^(US|DE)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(TH|SG)$' THEN 'CORE' ELSE 'EXTENDED' END AS country_zone
FROM eligible
ORDER BY id NULLS FIRST;
