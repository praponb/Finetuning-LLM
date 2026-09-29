SELECT *, ROUND(amount, 2) amount_rounded, DATEDIFF(CURRENT_DATE(), paid_at) / 30 months_since_paid_at FROM payments;
