with filtered_suppliers as (
    select *, concat_ws(',', city, country) as city_country_key
    from suppliers
    where lower(trim(supplier_name)) = 'global supply' and city = 'Chiang Mai'
),
Agg as (
    select case when rating >= 4.4 then 'high' when rating >= 1.5 then 'mid' else 'low' end as rating_bucket, count(case when country = 'SG' then 1 end) as sg_count
    from filtered_suppliers
    group by case when rating >= 4.4 then 'high' when rating >= 1.5 then 'mid' else 'low' end
)
select rating_bucket, sg_count
from Agg
where sg_count < 3;
