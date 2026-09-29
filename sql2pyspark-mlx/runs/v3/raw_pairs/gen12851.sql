select s.id, s.city, t.brand_list
from suppliers s
inner join (select supplier_id, sort_array(collect_set(brand)) as brand_list
    from products
    where stock_qty is not null
    group by supplier_id) t on s.id = t.supplier_id;
