WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount DESC, id) AS seq_no,
        MAX(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d
    FROM payments
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, order_id
    FROM enriched
    EXCEPT DISTINCT
    SELECT status, order_id
    FROM payments
    WHERE method = 'promptpay'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.status = k.status AND s.order_id = k.order_id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        amount_max_7d,
        LEAST(amount_max_7d, amount) AS least_amount_max_7d_amount,
        CASE WHEN amount >= 1500 THEN 'High' WHEN amount >= 530 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM verified
    WHERE paid_at > TO_DATE('2023-06-07')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, amount_max_7d + least_amount_max_7d_amount AS amount_max_7d_plus_least_amount_max_7d_amount
    FROM valid
    WHERE paid_at >= TIMESTAMP '2023-08-08 23:00:00'
      AND order_id IS NOT NULL
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        amount_max_7d,
        COUNT(least_amount_max_7d_amount) OVER (PARTITION BY order_id) AS order_id_count_least_amount_max_7d_amount,
        AVG(amount_max_7d) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_7d_avg_30d
    FROM cleaned
    WHERE amount < 1060
)
SELECT
    id,
    amount,
    paid_at,
    amount_max_7d,
    order_id_count_least_amount_max_7d_amount,
    amount_max_7d_avg_30d
FROM enriched_payments
ORDER BY amount_max_7d_avg_30d NULLS FIRST
