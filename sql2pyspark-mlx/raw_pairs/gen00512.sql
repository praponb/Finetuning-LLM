WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(signup_date, 60) AS signup_date_minus_60d,
        TRIM(country) AS clean_country
    FROM customers
    WHERE signup_date > DATE '2023-08-24'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY credit_limit NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
filtered_customers AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(city, 'Unknown') AS city_filled
    FROM deduped
    WHERE signup_date_minus_60d < '2024-06-01'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered_customers s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date >= s.signup_date_minus_60d
        )
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.started_at
    FROM kept s
    JOIN LATERAL (
            SELECT c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date_minus_60d
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        country,
        city,
        credit_limit,
        age,
        signup_date_minus_60d,
        city_filled,
        started_at,
        AVG(age) OVER w_running AS age_avg_5_prior,
        LEAD(age, 1) OVER (PARTITION BY country, segment ORDER BY signup_date_minus_60d, id) AS next_age
    FROM latest_match
    WHERE age BETWEEN 41 AND 62
    WINDOW w_running AS (PARTITION BY country, segment ORDER BY started_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY country ORDER BY age_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        LAG(next_age, 1) OVER (PARTITION BY city ORDER BY age, id) AS prev_next_age,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age_avg_5_prior DESC, id) AS seq_no
    FROM with_windows
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        age_avg_5_prior
    FROM enriched
) src
PIVOT (
    MAX(age_avg_5_prior) AS amount
    FOR country IN ('JP', 'UK')
);
