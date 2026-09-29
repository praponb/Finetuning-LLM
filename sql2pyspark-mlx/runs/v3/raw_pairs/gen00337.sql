select
    case when c.city in ('Bangkok', 'Tokyo') then 'group_a' else 'other' end as city_group,
    c.country,
    max(a.balance) - min(a.balance) as balance_range,
    max(a.opened_date) as last_opened_date
from accounts a
join customers c on a.customer_id = c.id
where c.name like '%Cha%'
group by case when c.city in ('Bangkok', 'Tokyo') then 'group_a' else 'other' end, c.country
order by city_group desc, country asc
limit 20;
