SELECT name, is_active, CONCAT_WS('-', name, segment) AS name_segment_key FROM customers WHERE LENGTH(email) >= 8
