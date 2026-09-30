select stores.region, round(avg(store_sales.units), 2) as avg_units, avg(store_sales.revenue) as avg_revenue
from store_sales
inner join products on store_sales.product_id = products.id
left join stores on store_sales.store_id = stores.id
where lower(stores.store_name) like '%ive%'
group by stores.region
order by avg_revenue desc, region desc;
