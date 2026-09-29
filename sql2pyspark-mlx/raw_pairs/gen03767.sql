SELECT 'order' AS kind, shipping_fee FROM orders WHERE shipping_fee IS NOT NULL UNION ALL SELECT 'transaction' AS kind, amount FROM transactions WHERE amount IS NOT NULL;
