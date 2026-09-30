select
    is_active,
    city,
    case when segment in ('Corporate', 'SMB') then 'group_a' else 'other' end as segment_group,
    max(credit_limit) as max_credit_limit
from customers
group by is_active, city, segment_group
having max(credit_limit) >= 5600
order by max_credit_limit desc;
