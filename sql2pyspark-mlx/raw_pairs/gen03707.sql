WITH latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.account_account_type,
        top1.account_opened_date
    FROM customers s
    JOIN LATERAL (
            SELECT c.account_type AS account_account_type, c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date <= s.account_opened_date
        )
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.subscription_start_date,
        top1.monthly_fee
    FROM kept s
    JOIN LATERAL (
            SELECT c.start_date AS subscription_start_date, c.monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY city ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        MIN(monthly_fee) OVER (PARTITION BY MONTH(signup_date), country) AS signup_date_month_min_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY account_account_type, city ORDER BY signup_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY signup_date DESC, id) AS seq_no
    FROM with_last
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    CASE WHEN name RLIKE '^(Noah Park|Kanya Srisuk)$' THEN 'GROUP_A' ELSE 'CORE' END AS name_zone
FROM scored
