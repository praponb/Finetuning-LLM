select *
from (
    select store_id, sum(units) total_units, min(units) min_units
    from store_sales
    group by store_id
) s
where min_units > 17
