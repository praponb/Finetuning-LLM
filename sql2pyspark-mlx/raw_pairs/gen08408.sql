select
    category,
    supplier_id,
    sum(case when brand = 'Umbrella' then stock_qty else 0 end) as umbrella_stock_qty,
    count(stock_qty) as stock_qty_count,
    sort_array(collect_set(brand)) as brand_list
from products
where price >= 740 and cost is not null and price >= 1070
group by cube(category, supplier_id);
