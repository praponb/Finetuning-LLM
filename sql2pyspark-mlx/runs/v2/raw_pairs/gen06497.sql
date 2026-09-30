SELECT
    id,
    method,
    amount,
    CONCAT_WS(',', status, method) AS status_method_key
FROM payments
