SELECT id, IF(is_flagged, 'Review', 'OK') AS review_status FROM transactions;
