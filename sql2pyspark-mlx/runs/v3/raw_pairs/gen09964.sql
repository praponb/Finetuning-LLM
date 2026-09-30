select case when c.country in ('JP', 'UK') then 'tier_1' else 'other' end country_group, c.is_active, avg(a.balance) avg_balance
from accounts a
left join customers c on a.customer_id = c.id
group by case when c.country in ('JP', 'UK') then 'tier_1' else 'other' end, c.is_active
order by country_group;
