SELECT
    id,
    amount,
    status,
    method,
    INITCAP(TRIM(method)) AS method_title,
    CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END AS is_promptpay
FROM payments
ORDER BY method_title;
