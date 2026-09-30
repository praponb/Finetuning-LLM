WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(email) AS email_lower
    FROM customers
    WHERE segment IS NOT NULL
      AND signup_date >= '2025-05-01'
),
with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.subscription_status,
        top1.plan
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.status AS subscription_status, c.plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        subscription_status,
        plan,
        CAST(is_active AS INT) AS is_active_flag
    FROM with_latest
    WHERE signup_date > DATE '2023-05-31'
      AND subscription_status IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, country
    FROM filtered
    EXCEPT
    SELECT city, country
    FROM customers
    WHERE LENGTH(email) >= 5
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.city = k.city AND s.country = k.country
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            subscription_status,
            segment,
            is_active_flag
        FROM verified
    ) src
    PIVOT (
        SUM(is_active_flag)
        FOR segment IN ('Consumer', 'Corporate', 'SMB')
    )
)
SELECT
    subscription_status,
    Consumer,
    Corporate,
    SMB
FROM pivoted
ORDER BY Consumer DESC
