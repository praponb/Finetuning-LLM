SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) status_list FROM payments WHERE order_id = 30 OR method IN ('bank_transfer', 'paypal', 'card') GROUP BY order_id;
