select *
from (
    select case when country in ('SG', 'UK') then 'group_a' else 'other' end as country_group, max(credit_limit) - min(credit_limit) as credit_limit_range
    from customers
    group by case when country in ('SG', 'UK') then 'group_a' else 'other' end
) as agg
where credit_limit_range >= 28
order by country_group;
