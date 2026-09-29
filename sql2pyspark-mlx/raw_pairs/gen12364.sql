WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST) AS dedup_rank
    FROM accounts
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        dedup_rank,
        LAST_VALUE(balance, TRUE) OVER w_recent AS last_known_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3
    FROM ranked
    WINDOW w_recent AS (PARTITION BY MONTH(opened_date), customer_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.account_type,
        a.opened_date,
        a.balance_avg_3,
        p.email,
        p.age
    FROM scored a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE p.segment = 'SMB'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.age,
        x.txn_type,
        x.amount
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.txn_type, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        txn_type,
        AVG(amount) OVER (PARTITION BY txn_type, customer_id) AS txn_type_avg_amount,
        ROUND(age / SUM(age) OVER (PARTITION BY customer_id), 4) AS age_share,
        ROUND(age / SUM(age) OVER (PARTITION BY txn_type), 4) AS age_share_2,
        SUM(age) OVER (PARTITION BY txn_type ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        AVG(amount) OVER (PARTITION BY customer_id, txn_type ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
    FROM with_latest
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, d.signup_date
    FROM scored_accounts a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
)
-- Join a dimension and aggregate
SELECT
    p.segment,
    p.country,
    COUNT(*) AS num_rows,
    SUM(src.age_share) AS total_age_share
FROM with_dims src
JOIN customers p ON src.customer_id = p.id
GROUP BY GROUPING SETS ((p.segment, p.country), (p.segment), ())
