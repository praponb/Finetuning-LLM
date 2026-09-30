select
    case when city in ('Tokyo', 'Singapore') then 'group_a' else 'other' end as city_group,
    is_active,
    sum(case when segment = 'SMB' then age else 0 end) as smb_age,
    avg(credit_limit) as avg_credit_limit
from customers
group by case when city in ('Tokyo', 'Singapore') then 'group_a' else 'other' end, is_active
having max(age) < 57;
