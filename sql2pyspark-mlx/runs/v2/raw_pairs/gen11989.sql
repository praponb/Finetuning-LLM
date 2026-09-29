SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.txn_type))) AS txn_type_values,
    SUM(CASE WHEN t.merchant = 'Amazon' THEN t.amount ELSE 0 END) AS amazon_amount
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE t.account_id = 5
GROUP BY a.id, a.account_type
HAVING amazon_amount >= 76
ORDER BY amazon_amount, id, account_type
LIMIT 5
