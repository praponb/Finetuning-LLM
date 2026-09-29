select
    s.id,
    s.rating,
    s.country,
    t.brand_values
from suppliers s
join (select supplier_id, concat_ws(', ', sort_array(collect_set(brand))) as brand_values
    from products
    group by supplier_id) t on s.id = t.supplier_id
