WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(email) AS email_upper,
        CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END AS is_smb
    FROM customers
    WHERE signup_date > TO_DATE('2023-03-02')
      AND city IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at <= s.signup_date
        )
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.status
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC NULLS LAST, id) AS row_num
        FROM with_last
    ) ranked
    WHERE row_num = 1
),
not_flagged_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY MONTH(signup_date), city ORDER BY credit_limit DESC) AS credit_limit_rank,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('YEAR', signup_date) ORDER BY age DESC, id) AS age_bucket,
        AVG(credit_limit) OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY MONTH(signup_date), segment ORDER BY signup_date, id) AS row_seq,
        MAX(credit_limit) OVER (PARTITION BY MONTH(signup_date), city) AS signup_date_month_max_credit_limit
    FROM not_flagged_customers
),
with_last_customers AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.signup_date,
        s.is_active,
        s.age,
        s.credit_limit_rank,
        s.age_bucket,
        s.row_seq,
        top1.account_status,
        top1.account_balance
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.status AS account_status, c.balance AS account_balance
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    city,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    AVG(age) FILTER (WHERE account_status = 'CLOSED') AS closed_avg_age,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM with_last_customers
GROUP BY DATE_TRUNC('MONTH', signup_date), city
HAVING AVG(age) FILTER (WHERE account_status = 'CLOSED') > 0
