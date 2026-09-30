select case when stores.sqft >= 14741 then 'high' when stores.sqft >= 3343 then 'mid' else 'low' end as sqft_bucket, min(store_sales.sale_date) as first_sale_date, sum(store_sales.units) as total_units
from store_sales
inner join stores on store_sales.store_id = stores.id
left join products on store_sales.product_id = products.id
group by case when stores.sqft >= 14741 then 'high' when stores.sqft >= 3343 then 'mid' else 'low' end
order by first_sale_date;
