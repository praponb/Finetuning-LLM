WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CONCAT_WS(',', status, method) AS status_method_key,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS DISTINCT FROM 'promptpay'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        method,
        paid_at,
        method_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at NULLS LAST, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest
    EXCEPT ALL
    SELECT id
    FROM payments
    WHERE method = 'cash'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.order_id,
        e.method,
        o.channel,
        o.amount
    FROM verified e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.amount <= 790
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(method, '/', channel) AS method_channel
    FROM enriched
    WHERE method NOT IN ('bank_transfer', 'cash')
)
SELECT
    s.order_id,
    s.channel,
    s.amount,
    CASE WHEN channel RLIKE '^(mobile|store)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS channel_zone,
    (SELECT SUM(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS sum_payment_amount
FROM cleaned s
ORDER BY channel DESC, order_id;
