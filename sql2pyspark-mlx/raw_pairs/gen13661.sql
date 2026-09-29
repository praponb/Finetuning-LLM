SELECT id, paid_at, amount, status, ROUND(amount, 0) AS amount_rounded FROM payments WHERE method = 'cash' OR status = 'SUCCESS' ORDER BY id DESC;
