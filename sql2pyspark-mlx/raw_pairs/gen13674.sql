WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(age) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age,
        MIN(age) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age,
        MAX(age) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_age,
        COUNT(age) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age
    FROM customers
    WHERE UPPER(TRIM(email)) = 'KANYA@MAIL.CO.TH'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        city,
        is_active,
        credit_limit,
        running_sum_age,
        running_max_age,
        running_count_age,
        UPPER(TRIM(email)) AS email_normalized,
        TRIM(name) AS clean_name
    FROM enriched
    WHERE signup_date >= TO_DATE('2023-08-21')
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.running_max_age,
        s.running_count_age,
        s.email_normalized,
        s.clean_name,
        x.account_account_type
    FROM valid s
    JOIN LATERAL (
            SELECT c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.monthly_fee < 200
        )
)
-- Multi-level aggregation
SELECT
    account_account_type,
    CASE WHEN running_max_age >= 58 THEN 'LARGE' ELSE 'SMALL' END AS running_max_age_tier,
    SUM(CASE WHEN account_account_type = 'CREDIT' THEN running_count_age ELSE 0 END) AS credit_running_count_age,
    AVG(running_max_age) FILTER (WHERE account_account_type = 'LOAN') AS loan_avg_running_max_age,
    PERCENTILE_APPROX(running_max_age, ARRAY(0.25, 0.5, 0.75)) AS running_max_age_quartiles,
    SORT_ARRAY(COLLECT_SET(account_account_type)) AS account_account_type_list,
    SUM(running_max_age) AS total_running_max_age,
    COUNT(*) FILTER (WHERE account_account_type = 'CREDIT') AS credit_rows,
    GROUPING_ID() AS gid
FROM kept
GROUP BY ROLLUP (account_account_type, CASE WHEN running_max_age >= 58 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY loan_avg_running_max_age DESC NULLS LAST
