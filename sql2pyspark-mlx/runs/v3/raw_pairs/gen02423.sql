with Src as (
    select *
    from store_sales
    where month(sale_date) in (3, 9, 10)
),
summary as (
    select case when revenue >= 3000 then 'high' when revenue >= 2000 then 'mid' else 'low' end revenue_bucket, date_trunc('MONTH', sale_date) sale_month, round(avg(revenue), 2) avg_revenue
    from Src
    group by case when revenue >= 3000 then 'high' when revenue >= 2000 then 'mid' else 'low' end, date_trunc('MONTH', sale_date)
)
select *
from summary
where avg_revenue >= 28
order by revenue_bucket, sale_month
limit 5
