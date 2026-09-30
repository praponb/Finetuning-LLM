select *
from (
    select case when status in ('SHIPPED', 'PENDING') then 'priority' else 'other' end as status_group, round(avg(discount), 2) as avg_discount
    from orders
    group by case when status in ('SHIPPED', 'PENDING') then 'priority' else 'other' end
) as x
where avg_discount < 64
order by status_group;
