WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE signup_date > TO_DATE('2023-08-02')
      AND segment IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date >= s.signup_date
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(email, ' - ', country) AS email_country,
        COALESCE(name, 'unassigned') AS name_filled,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM not_flagged
    WHERE segment IS NOT DISTINCT FROM 'SMB'
      AND signup_date >= '2023-09-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(credit_limit) OVER w_recent AS prev_credit_limit,
        LEAD(is_active_flag) OVER w_recent AS next_is_active_flag,
        ROW_NUMBER() OVER w_recent AS event_seq,
        CORR(credit_limit, age) OVER (PARTITION BY segment) AS corr_credit_limit_age
    FROM base
    WINDOW w_recent AS (PARTITION BY country ORDER BY signup_date, id)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    COUNT_IF(segment = 'SMB') AS smb_events,
    SUM(CASE WHEN city = 'New York' THEN age ELSE 0 END) AS new_york_age,
    SUM(event_seq) AS total_event_seq,
    CORR(age, credit_limit) AS corr_age_credit_limit,
    MIN(next_is_active_flag) AS min_next_is_active_flag,
    ROUND(AVG(corr_credit_limit_age), 2) AS avg_corr_credit_limit_age
FROM enriched
GROUP BY DATE_TRUNC('YEAR', signup_date);
