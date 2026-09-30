select
    case when revenue >= 3300 then 'high' when revenue >= 1100 then 'mid' else 'low' end as revenue_bucket,
    avg(revenue) as avg_revenue,
    max(sale_date) as latest_sale_date,
    max(units) - min(units) as units_range
from store_sales
where year(sale_date) = 2024
group by case when revenue >= 3300 then 'high' when revenue >= 1100 then 'mid' else 'low' end
order by revenue_bucket desc;
