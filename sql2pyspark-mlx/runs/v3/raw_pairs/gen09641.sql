SELECT *, CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END is_success, TRIM(method) clean_method
FROM payments;
