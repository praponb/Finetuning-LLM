select
    product_id,
    case when units >= 39 then 'high' when units >= 20 then 'mid' else 'low' end as units_bucket,
    count(distinct units) as unique_products,
    max(sale_date) as last_sale_date
from store_sales
where year(sale_date) = 2024
group by 1, 2
having count(*) > 1;
