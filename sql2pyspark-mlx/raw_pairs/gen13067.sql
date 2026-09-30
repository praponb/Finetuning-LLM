SELECT SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list, MIN(amount) AS min_amount FROM transactions ORDER BY min_amount DESC;
