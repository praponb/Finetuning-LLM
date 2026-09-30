with recent_store_sales as (
    select *
    from store_sales
    where revenue > 2900
),
summary as (
    select quarter(sale_date) sale_quarter, product_id, count(units) units_count
    from recent_store_sales
    group by quarter(sale_date), product_id
)
select sale_quarter, product_id, units_count
from summary
where units_count >= 10
order by units_count desc
