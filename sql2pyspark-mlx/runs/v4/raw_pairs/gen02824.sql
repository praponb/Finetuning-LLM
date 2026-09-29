select stores.region, max(store_sales.units) - min(store_sales.units) as units_range
from store_sales
join stores on store_sales.store_id = stores.id
group by stores.region
order by region
