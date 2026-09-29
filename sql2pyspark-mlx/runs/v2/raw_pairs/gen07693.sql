select
    date_format(sale_date, 'yyyy-MM') as sale_month,
    product_id,
    min(sale_date) as min_sale_date,
    max(units) - min(units) as units_range
from store_sales
where month(sale_date) in (1, 6, 12) and sale_date <= '2025-02-23' and units >= 39
group by date_format(sale_date, 'yyyy-MM'), product_id
having max(units) - min(units) >= 34
order by product_id desc, units_range desc
