WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(segment)) AS segment_title
    FROM customers
    WHERE signup_date >= DATE '2023-05-26'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT segment, city
    FROM base
    EXCEPT DISTINCT
    SELECT segment, city
    FROM customers
    WHERE credit_limit < 4500
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.segment = k.segment AND s.city = k.city
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        age - credit_limit AS age_minus_credit_limit,
        TRIM(country) AS clean_country,
        COALESCE(segment, 'N/A') AS segment_filled
    FROM reconciled
    WHERE signup_date >= DATE '2023-07-06'
      AND city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id = 8
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
        )
)
-- Grouped metrics
SELECT
    segment,
    BIT_AND(age) AS age_bit_and,
    SUM(CASE WHEN country = 'DE' THEN credit_limit ELSE 0 END) AS de_credit_limit,
    COUNT(DISTINCT country) AS distinct_countries,
    ROUND(AVG(age), 2) AS avg_age
FROM not_flagged
GROUP BY segment;
