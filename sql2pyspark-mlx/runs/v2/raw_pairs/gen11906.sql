SELECT account_id, merchant, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values FROM transactions GROUP BY account_id, merchant;
