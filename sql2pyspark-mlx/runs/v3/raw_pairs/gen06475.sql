SELECT monthly_fee AS value, 'subscription' AS source FROM subscriptions UNION ALL SELECT amount AS value, 'transaction' AS source FROM transactions WHERE amount IS NOT NULL ORDER BY source
