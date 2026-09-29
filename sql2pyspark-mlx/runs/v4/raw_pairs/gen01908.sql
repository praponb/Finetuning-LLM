WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(balance, 0) AS balance_rounded,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE customer_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS LAST, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance NULLS FIRST, id) AS dedup_rank
        FROM ranked
    ) t
    WHERE dedup_rank = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            account_type,
            balance_rounded
        FROM deduped
    ) src
    PIVOT (
        SUM(balance_rounded)
        FOR account_type IN ('CREDIT' AS credit, 'CHECKING' AS checking)
    )
)
SELECT
    s.customer_id,
    s.credit,
    s.checking,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM wide s
ORDER BY customer_id DESC NULLS LAST
LIMIT 20;
