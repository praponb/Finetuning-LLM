SELECT paid_at FROM payments WHERE NOT (status IN ('REFUNDED', 'FAILED', 'SUCCESS') AND amount <= 810) ORDER BY paid_at
