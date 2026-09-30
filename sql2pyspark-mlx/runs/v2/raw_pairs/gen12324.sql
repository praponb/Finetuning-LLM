select
    suppliers.city,
    case when suppliers.rating >= 3.8 then 'high' when suppliers.rating >= 1.5 then 'mid' else 'low' end as rating_bucket,
    sort_array(collect_set(products.brand)) as brand_list,
    sum(case when products.brand = 'Stark' then products.price else 0 end) as stark_price
from products
left join suppliers on suppliers.id = products.supplier_id
group by suppliers.city, case when suppliers.rating >= 3.8 then 'high' when suppliers.rating >= 1.5 then 'mid' else 'low' end
having sum(case when products.brand = 'Stark' then products.price else 0 end) >= 62
order by city, rating_bucket
limit 50
