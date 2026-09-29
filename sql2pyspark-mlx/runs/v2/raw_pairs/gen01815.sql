select
    id,
    product_id,
    units,
    date_add(sale_date, 24) sale_date_plus_24d
from store_sales
where units between 22 and 29
order by product_id, sale_date_plus_24d, id
limit 10;
