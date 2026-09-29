select
    stores.id,
    stores.city,
    t.unique_products,
    t.max_revenue
from stores
join (select store_id, count(distinct revenue) as unique_products, max(revenue) as max_revenue
    from store_sales
    group by store_id) t on stores.id = t.store_id
where t.unique_products > 4
order by max_revenue desc, id asc
limit 5;
