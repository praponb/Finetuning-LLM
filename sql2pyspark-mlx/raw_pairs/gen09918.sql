WITH filtered AS (
    SELECT
        *,
        ROUND(amount * 0.9, 2) adjusted_amount,
        COALESCE(method, 'unassigned') method_filled
    FROM payments
    WHERE method IS NOT DISTINCT FROM 'promptpay'
)
SELECT
    id,
    order_id,
    amount,
    paid_at,
    status,
    adjusted_amount,
    method_filled
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY adjusted_amount NULLS LAST, id) = 1
