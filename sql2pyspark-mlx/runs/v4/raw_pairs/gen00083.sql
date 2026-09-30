SELECT *, CONCAT_WS('/', method, status) AS method_status_key, INITCAP(TRIM(status)) AS status_title
FROM payments
