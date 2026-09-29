SELECT paid_at FROM payments WHERE method = 'card' AND (amount < 750 OR method <> 'promptpay') ORDER BY paid_at DESC;
