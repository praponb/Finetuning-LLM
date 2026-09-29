WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(paid_at) AS paid_at_hour,
        COALESCE(method, 'NONE') AS method_filled
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-11-05 08:00:00'
      AND method IS DISTINCT FROM 'card'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        amount,
        paid_at,
        status,
        paid_at_hour
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at_hour, id) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(paid_at_hour / amount, 2) AS paid_at_hour_per_amount,
        amount * paid_at_hour AS amount_times_paid_at_hour
    FROM deduped
    WHERE paid_at >= TIMESTAMP '2023-10-27 03:00:00'
      AND amount > 590
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_times_paid_at_hour NULLS FIRST, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM latest
    INTERSECT
    SELECT id, status
    FROM payments
    WHERE method IN ('paypal', 'promptpay')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(amount_times_paid_at_hour, 2) AS amount_times_paid_at_hour_rounded
    FROM kept_rows
    WHERE paid_at >= TO_DATE('2023-11-03')
      AND HOUR(paid_at) BETWEEN 11 AND 16
)
SELECT
    id,
    amount,
    status,
    paid_at_hour,
    amount_times_paid_at_hour,
    row_num
FROM cleaned_payments
ORDER BY amount;
