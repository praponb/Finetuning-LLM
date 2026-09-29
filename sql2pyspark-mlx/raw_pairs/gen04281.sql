SELECT paid_at, amount, ROUND(amount, 0) amount_rounded FROM payments ORDER BY amount, amount_rounded;
