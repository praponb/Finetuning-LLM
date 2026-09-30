select *
from (
    select store_id, max(units) as max_units
    from store_sales
    group by store_id
) as sub
where max_units > 32
order by 2 desc, store_id
limit 20;
