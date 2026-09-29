WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit + age AS credit_limit_plus_age,
        DATE_ADD(signup_date, 82) AS signup_date_plus_82d,
        COALESCE(segment, 'N/A') AS segment_filled
    FROM customers
    WHERE credit_limit IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(name) AS name_lower,
        COALESCE(city, 'N/A') AS city_filled,
        COALESCE(segment, 'NONE') AS resolved_segment
    FROM filtered
    WHERE signup_date >= TO_DATE('2023-03-18')
      AND city IS NOT NULL
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.status,
        latest.account_account_type
    FROM valid s
    JOIN LATERAL (
            SELECT c.status, c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, city
    FROM with_last
    EXCEPT DISTINCT
    SELECT country, city
    FROM customers
    WHERE name IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.city = k.city
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        is_active,
        credit_limit_plus_age,
        resolved_segment,
        status,
        account_account_type,
        MIN(credit_limit_plus_age) OVER (PARTITION BY status, country) AS status_min_credit_limit_plus_age,
        AVG(credit_limit_plus_age) OVER (PARTITION BY YEAR(signup_date), country) AS signup_date_year_avg_credit_limit_plus_age,
        MIN(credit_limit_plus_age) OVER (PARTITION BY segment) AS segment_min_credit_limit_plus_age
    FROM verified
)
-- Grouped metrics
SELECT
    CASE WHEN signup_date_year_avg_credit_limit_plus_age >= 70 THEN 'LARGE' ELSE 'SMALL' END AS signup_date_year_avg_credit_limit_plus_age_tier,
    SUM(signup_date_year_avg_credit_limit_plus_age) FILTER (WHERE account_account_type = 'CHECKING') AS checking_total_signup_date_year_avg_credit_limit_plus_age,
    AVG(segment_min_credit_limit_plus_age) FILTER (WHERE account_account_type = 'CHECKING') AS checking_avg_segment_min_credit_limit_plus_age,
    PERCENTILE_APPROX(segment_min_credit_limit_plus_age, 0.5) AS segment_min_credit_limit_plus_age_p50
FROM scored
GROUP BY CASE WHEN signup_date_year_avg_credit_limit_plus_age >= 70 THEN 'LARGE' ELSE 'SMALL' END;
