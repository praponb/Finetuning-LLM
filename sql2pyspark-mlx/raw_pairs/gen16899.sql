SELECT location label FROM departments EXCEPT SELECT method label FROM payments WHERE method IN ('paypal', 'bank_transfer') ORDER BY label
