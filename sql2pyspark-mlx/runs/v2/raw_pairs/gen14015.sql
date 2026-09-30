select
    s.id,
    s.supplier_name,
    count(p.id) as product_count,
    sort_array(collect_set(p.brand)) as brand_list
from suppliers s
left join products p on p.supplier_id = s.id
where p.stock_qty <= 100
group by s.id, s.supplier_name
order by product_count asc
