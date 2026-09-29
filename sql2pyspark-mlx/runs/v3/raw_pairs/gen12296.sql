SELECT *, INITCAP(TRIM(method)) AS method_title
FROM payments
WHERE order_id IS NOT NULL;
