SELECT id, order_id, INITCAP(TRIM(status)) AS status_title FROM payments WHERE order_id IN (5, 6, 12, 37) AND (amount IS NULL OR method = 'promptpay') ORDER BY id DESC, order_id DESC
