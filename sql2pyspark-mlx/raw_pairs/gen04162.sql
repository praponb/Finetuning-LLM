select a.account_type, count(t.id) as transaction_count from accounts a left join transactions t on a.id = t.account_id where t.merchant not in ('7-Eleven', 'Grab') group by a.account_type;
