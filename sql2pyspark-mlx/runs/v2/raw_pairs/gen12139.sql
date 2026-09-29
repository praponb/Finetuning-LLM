select *
from (
    select date_trunc('MONTH', sale_date) sale_month, min(sale_date) first_sale_date, sum(revenue) total_revenue
    from store_sales
    group by date_trunc('MONTH', sale_date)
) agg
where total_revenue < 18000
order by first_sale_date desc;
