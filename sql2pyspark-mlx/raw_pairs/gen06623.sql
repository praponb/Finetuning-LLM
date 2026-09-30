select s.city, t.category_list
from suppliers s
inner join (select supplier_id, sort_array(collect_set(category)) category_list
    from products
    where size(tags) >= 1
    group by supplier_id) t on s.id = t.supplier_id
order by city desc
