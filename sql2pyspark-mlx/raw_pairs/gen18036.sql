select
    supplier_id,
    brand,
    sort_array(collect_set(category)) as category_list,
    count(distinct cost) as unique_brands
from products
where category = 'Home'
group by rollup(supplier_id, brand)
order by unique_brands;
