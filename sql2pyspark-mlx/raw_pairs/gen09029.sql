WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        signup_date,
        is_active,
        credit_limit,
        age,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        COALESCE(segment, 'OTHER') AS segment_filled
    FROM customers
    WHERE signup_date BETWEEN '2024-06-01' AND '2025-09-01'
      AND segment IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.status = 'CANCELLED'
        )
),
valid_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        credit_limit,
        age,
        is_active_label,
        segment_filled,
        SUBSTRING(email, 1, 4) AS email_prefix
    FROM eligible
    WHERE signup_date >= DATE '2023-05-15'
      AND is_active
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(age, TRUE) OVER w_part AS last_known_age,
        SUM(age) OVER w_part AS cumulative_age
    FROM valid_customers
    WHERE age <> 52
    WINDOW w_part AS (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.customer_id IN (15, 21)
        )
),
valid_customers_2 AS (
    -- Basic cleansing and defaults
    SELECT *, REPLACE(email, ' ', '-') AS email_slug
    FROM eligible_customers
    WHERE age BETWEEN 39 AND 44
),
report AS (
    SELECT
        id,
        name,
        email,
        credit_limit,
        age,
        segment_filled,
        email_prefix,
        last_known_age,
        email_slug
    FROM valid_customers_2
    ORDER BY credit_limit, age DESC
)
SELECT
    id,
    email,
    credit_limit,
    segment_filled,
    email_prefix,
    last_known_age
FROM report
ORDER BY credit_limit
