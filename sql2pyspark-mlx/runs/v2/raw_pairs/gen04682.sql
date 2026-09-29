select *, lower(region) as region_lower, case when region = 'Central' then 1 else 0 end as is_central from stores where region = 'West' and region in ('East', 'North', 'South');
