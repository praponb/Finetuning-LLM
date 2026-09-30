WITH valid AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        UPPER(city) AS city_upper
    FROM customers
    WHERE city IS NOT NULL
),
base AS (
    SELECT
        id,
        name,
        email,
        segment,
        is_active,
        credit_limit,
        age,
        city_upper,
        LENGTH(email) AS email_length,
        TRIM(name) AS clean_name,
        COALESCE(city, 'N/A') AS city_filled
    FROM valid
    WHERE signup_date > TO_DATE('2023-04-19')
      AND city IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.monthly_fee <= 210
        )
),
metrics AS (
    SELECT
        *,
        NTILE(3) OVER w_recent AS credit_limit_bucket,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age, id) AS event_seq
    FROM eligible
    WHERE LOWER(email) LIKE '%lic%'
    WINDOW w_recent AS (PARTITION BY segment ORDER BY credit_limit, id)
)
SELECT
    segment,
    CASE WHEN age >= 66 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(credit_limit_bucket), 2) AS avg_credit_limit_bucket
FROM metrics
GROUP BY segment, CASE WHEN age >= 66 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_credit_limit_bucket >= 0;
