select
    t.id,
    t.is_flagged,
    t.txn_type,
    t.merchant,
    a.customer_id
from transactions t
left join accounts a on t.account_id = a.id
where t.merchant not in ('Amazon', '7-Eleven', 'Lazada') and t.is_flagged = true
order by t.txn_type
