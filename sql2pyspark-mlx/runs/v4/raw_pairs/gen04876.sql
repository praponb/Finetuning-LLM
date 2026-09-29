SELECT method, status FROM payments EXCEPT SELECT method, status FROM payments WHERE status IN ('FAILED', 'SUCCESS')
