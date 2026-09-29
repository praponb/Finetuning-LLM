WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-07-25 04:00:00'
      AND method IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS rank_in_group
    FROM sessionized
    QUALIFY rank_in_group = 1
),
valid_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        session_id * rank_in_group AS session_id_times_rank_in_group,
        session_id * rank_in_group AS session_id_times_rank_in_group_2,
        COALESCE(method, 'NONE') AS resolved_method
    FROM latest
    WHERE order_id IS NOT NULL
      AND method IS NOT DISTINCT FROM 'card'
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.amount AS order_amount,
        p.status
    FROM valid_payments src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
    WHERE p.status NOT IN ('COMPLETED', 'SHIPPED', 'RETURNED')
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM joined
    EXCEPT DISTINCT
    SELECT status, id
    FROM payments
    WHERE order_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT status, order_id
    FROM kept_rows
    INTERSECT
    SELECT status, order_id
    FROM payments
    WHERE status IN ('FAILED', 'SUCCESS')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN matched_keys_2 k ON s.status = k.status AND s.order_id = k.order_id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        method,
        order_amount
    FROM verified
) src
PIVOT (
    SUM(order_amount) AS value_sum,
    COUNT(1) AS cnt
    FOR method IN ('paypal', 'card')
)
