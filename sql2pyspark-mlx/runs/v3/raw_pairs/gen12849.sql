select a.id, count(t.id) as transaction_count from accounts a left join transactions t on t.account_id = a.id where t.is_flagged = true group by a.id order by id desc limit 10;
