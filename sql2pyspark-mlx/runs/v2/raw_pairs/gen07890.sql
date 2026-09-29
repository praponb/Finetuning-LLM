SELECT method, amount, paid_at, FLOOR(amount) amount_whole, DATEDIFF(CURRENT_DATE(), paid_at) days_since_paid_at FROM payments WHERE method != 'bank_transfer' AND method != 'card'
