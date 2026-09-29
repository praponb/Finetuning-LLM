WITH base AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE amount BETWEEN 700 AND 1210
      AND order_id IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY amount, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
latest AS (
    SELECT
        id,
        amount,
        paid_at,
        status,
        latest_rank
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, method ORDER BY paid_at DESC) AS row_num
        FROM deduped
    ) t
    WHERE row_num = 1
),
ranked AS (
    SELECT
        id,
        amount,
        paid_at
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS LAST) = 1
)
SELECT id
FROM ranked
ORDER BY id DESC NULLS LAST;
