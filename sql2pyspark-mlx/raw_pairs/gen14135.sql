WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        DATEDIFF(CURRENT_DATE(), paid_at) days_since_paid_at
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-10-24 11:00:00')
      AND method IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    method,
    amount,
    paid_at
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY days_since_paid_at DESC NULLS LAST, id) = 1;
