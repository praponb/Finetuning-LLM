with valid_suppliers as (
    select *
    from suppliers
    where city in ('Tokyo', 'New York') and supplier_name like '%Glo%'
),
suppliers_stats as (
    select country, case when rating >= 4.4 then 'high' when rating >= 1.6 then 'mid' else 'low' end as rating_bucket, count(distinct rating) as unique_countrys
    from valid_suppliers
    group by country, case when rating >= 4.4 then 'high' when rating >= 1.6 then 'mid' else 'low' end
)
select country, rating_bucket
from suppliers_stats
order by rating_bucket
