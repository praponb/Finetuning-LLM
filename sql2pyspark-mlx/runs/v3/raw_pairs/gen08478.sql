select a.id, a.account_type, count(t.id) as transaction_count
from accounts a
left join transactions t on a.id = t.account_id
where month(a.opened_date) in (2, 8, 12)
group by a.id, a.account_type;
