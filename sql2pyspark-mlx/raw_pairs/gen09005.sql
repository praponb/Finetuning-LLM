SELECT method, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM payments WHERE method IN ('paypal', 'cash') AND method IN ('promptpay', 'bank_transfer', 'cash') GROUP BY method ORDER BY method
