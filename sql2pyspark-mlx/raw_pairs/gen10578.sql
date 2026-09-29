WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT id, customer_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY opened_date DESC NULLS LAST, id) AS dedup_rank
        FROM accounts
    ) x
    WHERE dedup_rank = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (9, 14)
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.segment,
        d.city,
        d.name
    FROM not_flagged e
    INNER JOIN customers d
        ON e.customer_id <=> d.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        segment,
        name,
        CONCAT(segment, '/', name) AS segment_name,
        COALESCE(city, 'N/A') AS city_filled
    FROM enriched
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.segment_name,
        top1.transaction_amount,
        top1.transaction_txn_type
    FROM base s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    transaction_amount,
    transaction_txn_type
FROM with_last;
