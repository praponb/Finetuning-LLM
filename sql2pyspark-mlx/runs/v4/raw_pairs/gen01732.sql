select
    date_trunc('MONTH', a.opened_date) opened_month,
    a.account_type,
    sum(case when t.merchant = '7-Eleven' then 1 else 0 end) 7_eleven_count,
    sort_array(collect_set(t.txn_type)) txn_type_list
from transactions t
right join accounts a on t.account_id = a.id
join customers c on a.customer_id = c.id
where t.is_flagged = true
group by date_trunc('MONTH', a.opened_date), a.account_type
order by opened_month;
