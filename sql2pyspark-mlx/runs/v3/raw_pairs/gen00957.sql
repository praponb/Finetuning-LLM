SELECT a.account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.merchant))) AS merchant_values FROM transactions t INNER JOIN accounts a ON t.account_id = a.id GROUP BY a.account_type;
