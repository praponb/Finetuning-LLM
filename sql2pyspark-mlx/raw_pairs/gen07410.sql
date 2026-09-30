WITH base AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(account_type) AS clean_account_type
    FROM accounts
    WHERE account_type IN ('CREDIT', 'LOAN', 'SAVINGS')
      AND status IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IN ('Lazada', 'Shell', '7-Eleven')
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.segment,
        p.age,
        p.email
    FROM not_flagged a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        opened_date,
        status,
        clean_account_type,
        age,
        email
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment, customer_id ORDER BY age) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY status), 4) AS age_share,
        LEAD(age, 2) OVER (PARTITION BY MONTH(opened_date), status ORDER BY opened_date, id) AS next_age,
        LAG(age, 2) OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date), status ORDER BY opened_date, id) AS prev_age
    FROM ranked
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        email,
        age_share,
        prev_age,
        LEAD(next_age, 1) OVER (PARTITION BY status ORDER BY age_share, id) AS next_next_age,
        AVG(next_age) OVER w_recent AS next_age_avg_3,
        ROUND(prev_age / SUM(prev_age) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date)), 4) AS prev_age_share
    FROM eligible
    WINDOW w_recent AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    opened_date,
    email,
    age_share,
    next_next_age
FROM enriched_accounts
ORDER BY opened_date DESC;
