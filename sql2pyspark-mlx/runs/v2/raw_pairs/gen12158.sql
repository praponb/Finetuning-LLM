with filtered_store_sales as (
    select *
    from store_sales
    where store_id in (2, 6) and store_id in (3, 5) and month(sale_date) in (1, 3, 10)
),
summary as (
    select date_trunc('MONTH', sale_date) sale_month, count(*) num_store_sales, max(sale_date) last_sale_date
    from filtered_store_sales
    group by date_trunc('MONTH', sale_date)
)
select sale_month, num_store_sales, last_sale_date
from summary
where num_store_sales < 9;
