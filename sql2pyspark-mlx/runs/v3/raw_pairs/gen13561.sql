SELECT monthly_fee, 'subscription' AS source FROM subscriptions UNION ALL SELECT amount, 'transaction' AS source FROM transactions WHERE merchant IS NULL
