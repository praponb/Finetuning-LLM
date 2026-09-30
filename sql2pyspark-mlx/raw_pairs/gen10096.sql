SELECT
    amount,
    status,
    method,
    INITCAP(TRIM(status)) status_title
FROM payments;
