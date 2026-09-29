select a.opened_date, t.is_flagged
from accounts a
left join transactions t on a.id = t.account_id
where t.txn_time < '2025-01-01'
order by opened_date asc
