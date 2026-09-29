WITH filtered AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        INITCAP(TRIM(method)) method_title,
        COALESCE(method, 'N/A') method_filled
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS DISTINCT FROM 'cash'
)
SELECT
    *,
    AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) amount_avg_6_prior,
    LEAD(amount, 1) OVER (PARTITION BY MONTH(paid_at) ORDER BY amount, id) next_amount
FROM filtered
WHERE HOUR(paid_at) BETWEEN 7 AND 10;
