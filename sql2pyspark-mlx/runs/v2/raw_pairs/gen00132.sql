SELECT *, LOWER(method) AS method_lower FROM payments WHERE method <> 'promptpay'
