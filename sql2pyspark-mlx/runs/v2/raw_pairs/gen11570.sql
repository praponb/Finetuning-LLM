SELECT
    id,
    amount,
    status,
    SUBSTRING(method, 1, 3) AS method_prefix
FROM payments
WHERE amount >= 570 AND amount > 1010
ORDER BY amount ASC, method_prefix ASC
