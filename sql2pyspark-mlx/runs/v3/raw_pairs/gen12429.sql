select
    stores.region,
    stores.sqft,
    stores.opened_date,
    store_sales.sale_date
from stores
left join store_sales on stores.id = store_sales.store_id
where store_sales.product_id in (6, 7)
order by opened_date;
