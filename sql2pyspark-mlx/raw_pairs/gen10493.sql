WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(status)) status_title,
        TO_DATE(paid_at) paid_at_date
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-09-05 03:00:00'
)
SELECT *
FROM (
    SELECT
        order_id,
        method,
        amount
    FROM filtered
) src
PIVOT (
    MAX(amount)
    FOR method IN ('paypal', 'card')
);
