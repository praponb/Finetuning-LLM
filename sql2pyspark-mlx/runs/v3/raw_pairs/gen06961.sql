select *
from (
    select store_id, max(units) - min(units) as units_range
    from store_sales
    group by store_id
) as s
where units_range > 51
order by units_range desc;
