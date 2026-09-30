SELECT is_flagged, SUM(amount) AS total_amount, ROUND(AVG(amount), 2) AS avg_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values FROM transactions GROUP BY is_flagged;
