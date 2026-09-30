select case when s.country in ('JP', 'DE') then 'tier_1' else 'other' end as country_group, s.country, round(avg(p.price), 2) as avg_price
from products p
join suppliers s on p.supplier_id = s.id
group by case when s.country in ('JP', 'DE') then 'tier_1' else 'other' end, s.country
having round(avg(p.price), 2) >= 27
order by country_group;
