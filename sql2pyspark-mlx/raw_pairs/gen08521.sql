with RecentSuppliers as (
    select *, coalesce(rating, 0) as rating_filled
    from suppliers
    where city = 'Singapore' and lower(supplier_name) like '%oky%'
),
country_group_summary as (
    select case when country in ('UK', 'SG') then 'priority' else 'other' end as country_group, count(distinct rating) as unique_citys
    from RecentSuppliers
    group by case when country in ('UK', 'SG') then 'priority' else 'other' end
)
select country_group, unique_citys
from country_group_summary
where unique_citys < 4
order by unique_citys, country_group
limit 1
