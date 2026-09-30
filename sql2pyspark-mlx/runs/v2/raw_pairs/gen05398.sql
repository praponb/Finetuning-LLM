SELECT id, amount, ROUND(amount, 0) AS amount_rounded FROM transactions ORDER BY id DESC, amount
