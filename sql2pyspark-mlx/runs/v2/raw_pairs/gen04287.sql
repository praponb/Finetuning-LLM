select
    year(sale_date) as sale_year,
    store_id,
    count(distinct revenue) as unique_products,
    min(sale_date) as first_sale_date
from store_sales
group by year(sale_date), store_id
order by store_id desc;
