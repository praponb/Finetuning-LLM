SELECT *, INITCAP(TRIM(status)) AS status_title, CAST(amount AS INT) AS amount_int
FROM payments
WHERE order_id = 2 AND method IN ('cash', 'bank_transfer', 'card')
