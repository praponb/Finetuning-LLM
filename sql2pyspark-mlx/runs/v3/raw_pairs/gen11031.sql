select cost, price, launch_date, lower(category) as category_lower, case when brand = 'Stark' then 1 else 0 end as is_stark from products order by price desc;
