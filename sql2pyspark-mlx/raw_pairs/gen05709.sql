WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY method, order_id ORDER BY paid_at, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY paid_at, id) AS seq_no,
        NTILE(3) OVER (ORDER BY amount DESC, id) AS amount_bucket
    FROM payments
),
kept_keys AS (
    -- Set difference on keys
    SELECT status
    FROM metrics
    EXCEPT ALL
    SELECT status
    FROM payments
    WHERE order_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.status = k.status
),
matched_keys AS (
    -- Set difference on keys
    SELECT method, id
    FROM verified
    EXCEPT
    SELECT method, id
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 12 AND 19
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN matched_keys k ON s.method = k.method AND s.id = k.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        status,
        next_amount,
        seq_no,
        amount_bucket,
        ROUND(amount_bucket / amount, 2) AS amount_bucket_per_amount,
        next_amount + amount AS next_amount_plus_amount,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM reconciled
    WHERE method IS DISTINCT FROM 'cash'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN next_amount_plus_amount >= 65 THEN 'High' WHEN next_amount_plus_amount >= 32 THEN 'Medium' ELSE 'Low' END AS next_amount_plus_amount_level
    FROM filtered
    WHERE order_id IS NOT NULL
)
SELECT
    s.id,
    s.order_id,
    s.next_amount,
    s.seq_no,
    s.next_amount_plus_amount,
    s.next_amount_plus_amount_level,
    (SELECT MIN(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS min_shipment_cost
FROM valid s
