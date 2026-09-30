SELECT amount, LENGTH(status) AS status_length FROM payments WHERE method = 'bank_transfer' ORDER BY amount DESC;
