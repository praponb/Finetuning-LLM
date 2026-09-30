select
    t.amount,
    t.account_id,
    a.opened_date,
    a.customer_id,
    c.country,
    initcap(trim(t.txn_type)) as txn_type_title
from transactions t
left join accounts a on a.id = t.account_id
inner join customers c on a.customer_id = c.id
where length(c.name) > 7
order by a.opened_date;
