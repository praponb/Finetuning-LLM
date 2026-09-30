select
    case when method in ('bank_transfer', 'paypal') then 'priority' else 'other' end as method_group,
    case when amount >= 1450 then 'high' when amount >= 700 then 'mid' else 'low' end as amount_bucket,
    count(distinct amount) as unique_methods,
    count(distinct amount) as unique_statuss
from payments
group by case when method in ('bank_transfer', 'paypal') then 'priority' else 'other' end, case when amount >= 1450 then 'high' when amount >= 700 then 'mid' else 'low' end
having count(distinct amount) >= 2
