WITH deduped AS (
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        credit_limit
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC NULLS LAST) AS dedup_rank
        FROM customers
    ) ranked
    WHERE dedup_rank = 1
),
with_latest AS (
    SELECT
        s.*,
        top1.subscription_monthly_fee,
        top1.end_date
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee AS subscription_monthly_fee, c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    SELECT s.*, top1.status
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept_keys AS (
    SELECT id, segment
    FROM with_last
    EXCEPT ALL
    SELECT id, segment
    FROM customers
    WHERE email LIKE '%@mail.co.th'
),
reconciled AS (
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.id = k.id AND s.segment = k.segment
),
not_flagged AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.browser = 'Chrome'
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IS NOT NULL
        )
)
SELECT
    id,
    email,
    city,
    signup_date,
    credit_limit,
    status
FROM kept
ORDER BY id NULLS FIRST;
