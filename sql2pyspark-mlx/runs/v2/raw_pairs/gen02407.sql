SELECT *, INITCAP(TRIM(status)) AS status_title, CONCAT_WS('-', status, method) AS status_method_key
FROM payments
ORDER BY status_method_key DESC;
