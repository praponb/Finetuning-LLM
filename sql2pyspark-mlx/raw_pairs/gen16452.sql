SELECT opened_date, 'account' AS origin FROM accounts WHERE opened_date IS NOT NULL UNION ALL SELECT last_restock, 'inventory' AS origin FROM inventory WHERE last_restock IS NOT NULL
