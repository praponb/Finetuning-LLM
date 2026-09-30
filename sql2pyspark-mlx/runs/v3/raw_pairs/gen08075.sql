select
    store_id,
    quarter(sale_date) as sale_quarter,
    count(*) as store_sale_count,
    sum(units) as total_units
from store_sales
where product_id in (3, 4)
group by store_id, sale_quarter
