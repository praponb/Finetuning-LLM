WITH filtered AS (
    SELECT
        *,
        CASE WHEN method IN ('card', 'bank_transfer') THEN 'Group A' ELSE 'Other' END AS method_group,
        UPPER(status) AS status_upper,
        COALESCE(method, 'NONE') AS resolved_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-11-09 08:00:00'
)
SELECT
    p.channel,
    p.status AS order_status,
    src.status AS payment_status,
    COUNT(*) AS rows_total,
    ROUND(SUM(src.amount), 2) AS total_amount
FROM filtered src
JOIN orders p ON src.order_id = p.id
GROUP BY p.channel, p.status, src.status
ORDER BY payment_status DESC NULLS FIRST;
