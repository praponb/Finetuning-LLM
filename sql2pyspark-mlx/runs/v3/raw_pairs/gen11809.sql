select a.status, count(t.id) as transaction_count from accounts a left outer join transactions t on a.id = t.account_id group by a.status order by transaction_count desc
