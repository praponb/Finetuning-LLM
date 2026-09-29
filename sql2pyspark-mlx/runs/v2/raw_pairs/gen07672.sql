select
    store_id,
    sale_quarter,
    num_store_sales,
    avg_units
from (
    select
        store_id,
        quarter(sale_date) as sale_quarter,
        count(*) as num_store_sales,
        avg(units) as avg_units
    from store_sales
    group by store_id, quarter(sale_date)
) as t
where num_store_sales >= 9
order by sale_quarter
