with store_summary as (
    select store_id, min(sale_date) as min_sale_date, min(revenue) as min_revenue
    from store_sales
    group by store_id
)
select
    stores.id,
    stores.opened_date,
    store_summary.min_sale_date,
    store_summary.min_revenue
from stores
join store_summary on stores.id = store_summary.store_id
where store_summary.min_revenue > 1000;
