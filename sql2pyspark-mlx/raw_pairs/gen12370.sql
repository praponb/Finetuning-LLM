select sale_month, min_revenue, min_units
from (
    select date_format(sale_date, 'yyyy-MM') as sale_month, min(revenue) as min_revenue, min(units) as min_units
    from store_sales
    group by date_format(sale_date, 'yyyy-MM')
) as sub
where min_revenue > 1700;
