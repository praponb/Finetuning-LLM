select
    case when c.age >= 62 then 'high' when c.age >= 30 then 'mid' else 'low' end as age_bucket,
    case when c.credit_limit >= 13800 then 'high' when c.credit_limit >= 2800 then 'mid' else 'low' end as credit_limit_bucket,
    count(s.monthly_fee) as monthly_fee_count,
    sort_array(collect_set(s.status)) as status_list
from subscriptions s
inner join customers c on s.customer_id = c.id
where c.name like 'Mi%'
group by case when c.age >= 62 then 'high' when c.age >= 30 then 'mid' else 'low' end, case when c.credit_limit >= 13800 then 'high' when c.credit_limit >= 2800 then 'mid' else 'low' end
