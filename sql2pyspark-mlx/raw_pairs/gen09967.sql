WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date BETWEEN sub.start_date AND sub.end_date
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.amount,
        latest.transaction_merchant
    FROM attributed s
    JOIN LATERAL (
            SELECT c.amount, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, b.age
    FROM with_latest s
    INNER JOIN customers b
        ON s.customer_id = b.id
    WHERE b.is_active = TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_merchant ORDER BY balance DESC, id, subscription_id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.opened_date,
        s.subscription_id,
        s.transaction_merchant,
        s.age,
        top1.transaction_merchant_2
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_merchant_3
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant_3
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, d.age AS customer_age
    FROM latest_match src
    INNER JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    id,
    customer_id,
    opened_date,
    subscription_id,
    transaction_merchant,
    transaction_merchant_2,
    transaction_merchant_3,
    CASE WHEN transaction_merchant_3 RLIKE '^(Grab|7-Eleven)$' THEN 'GROUP_B' ELSE 'CORE' END AS transaction_merchant_3_zone
FROM joined
ORDER BY transaction_merchant DESC NULLS LAST;
