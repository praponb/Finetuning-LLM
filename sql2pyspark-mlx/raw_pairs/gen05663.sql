select case when country in ('DE', 'TH') then 'group_a' else 'other' end as country_group, country, min(age) as min_age from customers where name is not null group by country_group, country
