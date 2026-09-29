WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(segment) AS clean_segment
    FROM customers
    WHERE signup_date >= TO_DATE('2023-02-17')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND YEAR(ch.end_date) >= 2024
        )
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.account_status
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        clean_segment
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country, account_status ORDER BY age) AS latest_rank
        FROM with_latest
    ) ranked
    WHERE latest_rank = 1
),
with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.is_active,
        s.clean_segment,
        top1.session_device,
        top1.session_started_at
    FROM latest s
    JOIN LATERAL (
            SELECT c.device AS session_device, c.started_at AS session_started_at
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM with_last
    EXCEPT
    SELECT id, city
    FROM customers
    WHERE segment NOT IN ('Consumer', 'SMB')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
cleaned_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        is_active,
        session_started_at,
        TO_DATE(session_started_at) AS session_started_at_date
    FROM reconciled
    WHERE city IS DISTINCT FROM 'Tokyo'
      AND session_device IN ('desktop', 'tablet')
)
-- Grouped metrics
SELECT
    country,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count
FROM cleaned_customers
GROUP BY country
ORDER BY cnt DESC;
