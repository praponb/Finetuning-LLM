select
    store_id,
    quarter(sale_date) sale_quarter,
    count(revenue) revenue_count,
    min(revenue) min_revenue
from store_sales
group by store_id, quarter(sale_date)
