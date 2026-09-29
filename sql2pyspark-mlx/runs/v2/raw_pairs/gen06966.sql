select
    a.id,
    a.status,
    a.opened_date,
    t.merchant_list,
    t.total_count
from accounts a
inner join (select account_id, sort_array(collect_set(merchant)) as merchant_list, count(*) as total_count
    from transactions
    group by account_id) t on a.id = t.account_id
where t.total_count > 9;
