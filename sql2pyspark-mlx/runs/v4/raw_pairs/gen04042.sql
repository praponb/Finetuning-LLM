select t.merchant, t.amount, t.txn_type, a.status, trim(a.account_type) as clean_account_type from transactions t join accounts a on t.account_id = a.id where year(a.opened_date) = 2023;
