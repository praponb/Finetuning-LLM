select a.status, count(case when t.txn_type = 'REFUND' then 1 end) as refund_count, max(t.amount) - min(t.amount) as amount_range
from transactions t
join accounts a on a.id = t.account_id
where t.is_flagged = true
group by a.status
having count(case when t.txn_type = 'REFUND' then 1 end) > 4
order by a.status asc, refund_count asc
