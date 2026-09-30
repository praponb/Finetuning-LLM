with supplier_stats as (
    select supplier_id, concat_ws(', ', sort_array(collect_set(brand))) as brand_values
    from products
    group by supplier_id
)
select suppliers.country, suppliers.city, supplier_stats.brand_values
from suppliers
inner join supplier_stats on suppliers.id = supplier_stats.supplier_id
order by supplier_stats.brand_values desc;
