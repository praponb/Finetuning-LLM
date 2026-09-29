WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        COALESCE(city, 'N/A') AS city_filled,
        CASE WHEN city IN ('Singapore', 'Berlin') THEN 'Core' ELSE 'Other' END AS city_group,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM customers
    WHERE signup_date >= DATE '2023-02-06'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(name) AS name_length,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain,
        COALESCE(city, 'NONE') AS resolved_city
    FROM cleaned
    WHERE is_active = TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        signup_date,
        is_active,
        city_filled,
        city_group,
        segment_clean,
        email_domain,
        resolved_city,
        SUM(credit_limit) OVER (ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        LEAD(name_length, 1) OVER (ORDER BY signup_date ASC, id ASC) AS next_name_length
    FROM base
    WHERE signup_date BETWEEN '2024-06-01' AND '2024-12-01'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.signup_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        signup_date,
        city_group,
        segment_clean,
        email_domain,
        resolved_city,
        running_credit_limit,
        SUM(running_credit_limit) OVER (ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_credit_limit,
        DENSE_RANK() OVER (ORDER BY running_credit_limit DESC NULLS FIRST) AS running_credit_limit_dense_rank,
        SUM(next_name_length) OVER (PARTITION BY YEAR(signup_date) ORDER BY next_name_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_name_length,
        LAST_VALUE(next_name_length, TRUE) OVER (ORDER BY running_credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_name_length
    FROM kept
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.status IN ('CLOSED', 'FROZEN')
        )
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN last_known_next_name_length >= 81 THEN 'LARGE' ELSE 'SMALL' END AS last_known_next_name_length_tier,
        APPROX_PERCENTILE(running_next_name_length, ARRAY(0.25, 0.5, 0.75)) AS running_next_name_length_quartiles,
        COUNT(*) AS cnt,
        ROUND(AVG(running_running_credit_limit), 2) AS avg_running_running_credit_limit
    FROM not_flagged
    GROUP BY CASE WHEN last_known_next_name_length >= 81 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    last_known_next_name_length_tier,
    running_next_name_length_quartiles,
    cnt,
    avg_running_running_credit_limit
FROM grouped;
