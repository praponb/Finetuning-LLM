with recent_store_sales as (
    select *
    from store_sales
    where units != 38
),
StoreSalesStats as (
    select date_format(sale_date, 'yyyy-MM') as sale_month, round(avg(revenue), 2) as avg_revenue
    from recent_store_sales
    group by date_format(sale_date, 'yyyy-MM')
)
select sale_month, avg_revenue
from StoreSalesStats
where avg_revenue < 62
order by avg_revenue, sale_month
limit 3
