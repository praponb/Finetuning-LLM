WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(age) OVER w_recent AS last7_avg_age,
        MIN(age) OVER w_recent AS last7_min_age,
        COUNT(age) OVER w_recent AS last7_count_age,
        MAX(age) OVER w_recent AS last7_max_age,
        AVG(age) OVER w_running AS age_avg_3_prior
    FROM customers
    WINDOW w_recent AS (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY segment, country ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        signup_date,
        age,
        last7_min_age,
        last7_count_age,
        last7_max_age,
        age_avg_3_prior,
        AVG(last7_avg_age) OVER (PARTITION BY country ORDER BY last7_count_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last7_avg_age,
        MIN(last7_avg_age) OVER (PARTITION BY country ORDER BY last7_count_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last7_avg_age,
        MAX(last7_avg_age) OVER (PARTITION BY country ORDER BY last7_count_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last7_avg_age,
        SUM(last7_avg_age) OVER (PARTITION BY country ORDER BY last7_count_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last7_avg_age
    FROM metrics
    WHERE age BETWEEN 34 AND 66
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
        )
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.last7_min_age,
        s.last7_max_age,
        s.age_avg_3_prior,
        s.running_sum_last7_avg_age,
        x.balance,
        x.account_type
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.balance, c.account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.session_device,
        latest.session_browser
    FROM with_last s
    JOIN LATERAL (
            SELECT c.device AS session_device, c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.account_type IN ('SAVINGS', 'CHECKING')
        )
),
metrics_customers AS (
    -- Ranking and gap features
    SELECT
        id,
        last7_min_age,
        running_sum_last7_avg_age,
        session_device,
        LAG(last7_min_age, 1, 0) OVER (PARTITION BY country ORDER BY balance, id) AS prev_last7_min_age,
        LEAD(last7_max_age, 1) OVER (ORDER BY age_avg_3_prior, id) AS next_last7_max_age,
        SUM(last7_max_age) OVER w_grp AS running_last7_max_age,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM not_flagged_customers
    WINDOW w_grp AS (PARTITION BY session_browser ORDER BY last7_min_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY account_type ORDER BY running_sum_last7_avg_age, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_sum_last7_avg_age >= 303 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_last7_avg_age_tier,
    CASE WHEN last7_min_age >= 59 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_min_age_tier,
    session_device,
    COUNT(DISTINCT session_device) AS distinct_session_devices,
    MIN(running_last7_max_age) AS min_running_last7_max_age,
    SORT_ARRAY(COLLECT_SET(session_device)) AS session_device_list
FROM metrics_customers
GROUP BY 1, 2, 3
HAVING min_running_last7_max_age < 1000000
ORDER BY distinct_session_devices NULLS LAST
