select
    date_trunc('MONTH', sale_date) as sale_month,
    case when units >= 43 then 'high' when units >= 19 then 'mid' else 'low' end as units_bucket,
    max(sale_date) as last_sale_date,
    count(distinct revenue) as unique_stores,
    max(units) as max_units
from store_sales
where product_id = 8
group by date_trunc('MONTH', sale_date), case when units >= 43 then 'high' when units >= 19 then 'mid' else 'low' end
having unique_stores >= 3;
