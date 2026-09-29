WITH filtered AS (
    SELECT
        *,
        ROUND(credit_limit, 2) AS credit_limit_rounded,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM customers
    WHERE segment IS NOT NULL
      AND segment IS DISTINCT FROM 'Consumer'
),
scored AS (
    SELECT
        id,
        name,
        email,
        city,
        signup_date,
        credit_limit,
        is_active_label,
        city_filled,
        COUNT(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        SUM(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age,
        MIN(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age
    FROM filtered
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
),
latest AS (
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_sum_age NULLS FIRST, id) = 1
),
filtered_customers AS (
    SELECT
        id,
        name,
        city,
        signup_date,
        credit_limit,
        is_active_label,
        city_filled,
        running_sum_age,
        running_min_age,
        LEAST(running_count_age, running_sum_age) AS least_running_count_age_running_sum_age,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM latest
    WHERE city IS NOT NULL
),
with_last AS (
    SELECT s.*, x.account_opened_date
    FROM filtered_customers s
    JOIN LATERAL (
            SELECT c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    city,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    APPROX_PERCENTILE(credit_limit, 0.5) AS credit_limit_p50,
    RANK() OVER (PARTITION BY city ORDER BY COUNT_IF(city = 'Chiang Mai') DESC) AS chiang_mai_events_rank
FROM with_last
GROUP BY city, DATE_TRUNC('YEAR', signup_date)
HAVING credit_limit_p50 >= 0
ORDER BY city
