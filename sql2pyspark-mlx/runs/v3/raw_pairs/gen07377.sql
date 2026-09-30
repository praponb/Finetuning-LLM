SELECT a.customer_id, SORT_ARRAY(COLLECT_SET(t.merchant)) AS merchant_list FROM transactions t RIGHT OUTER JOIN accounts a ON t.account_id = a.id GROUP BY a.customer_id ORDER BY a.customer_id DESC
