SELECT accounts.id, accounts.opened_date, t.merchant_list
FROM accounts
INNER JOIN (SELECT account_id, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM transactions
    WHERE is_flagged = FALSE
    GROUP BY account_id) t ON accounts.id = t.account_id;
