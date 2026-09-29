WITH base AS (
    SELECT
        *,
        CAST(balance AS INT) AS balance_int,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE opened_date >= DATE '2023-08-27'
      AND account_type = 'CREDIT'
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.balance,
        src.opened_date,
        src.balance_int,
        src.resolved_status,
        d.credit_limit,
        d.email,
        d.segment
    FROM base src
    LEFT JOIN customers d
        ON src.customer_id = d.id
    WHERE d.segment IN ('SMB', 'Consumer')
),
filtered AS (
    SELECT
        id,
        customer_id,
        opened_date,
        balance_int,
        resolved_status,
        credit_limit,
        email,
        ROUND(credit_limit * 0.9, 2) AS credit_limit_adjusted,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM enriched
    WHERE MONTH(opened_date) IN (2, 8, 9)
),
with_dims AS (
    SELECT
        a.id,
        a.customer_id,
        a.credit_limit_adjusted,
        p.email AS customer_email
    FROM filtered a
    JOIN customers p
        ON a.customer_id = p.id
),
eligible AS (
    SELECT s.*
    FROM with_dims s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (16, 29)
        )
)
SELECT
    customer_id,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    MIN(credit_limit_adjusted) AS min_credit_limit_adjusted,
    MIN(credit_limit_adjusted) AS min_credit_limit_adjusted_2
FROM eligible
GROUP BY customer_id
HAVING min_credit_limit_adjusted_2 >= 0
ORDER BY min_credit_limit_adjusted NULLS FIRST
