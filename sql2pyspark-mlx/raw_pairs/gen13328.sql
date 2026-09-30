WITH joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.city,
        d.country
    FROM accounts s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.city = 'Singapore'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY opened_date DESC NULLS LAST, id) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance, 1) OVER (PARTITION BY country ORDER BY balance, id) prev_balance,
        SUM(balance) OVER (PARTITION BY DATE_TRUNC('week', opened_date)) opened_date_period_sum_balance,
        FIRST_VALUE(city, TRUE) OVER w_hist first_known_city
    FROM ranked
    WINDOW w_hist AS (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.rank_in_group,
        s.prev_balance,
        s.opened_date_period_sum_balance,
        s.first_known_city,
        top1.amount,
        top1.transaction_txn_type
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined_accounts AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.account_type,
        e.balance,
        e.rank_in_group,
        e.first_known_city,
        d.country,
        d.email
    FROM latest_match e
    INNER JOIN customers d
        ON e.customer_id <=> d.id
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.email,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM joined_accounts s
