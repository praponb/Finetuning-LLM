SELECT channel, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM orders WHERE shipping_fee > 27 GROUP BY channel ORDER BY status_values
