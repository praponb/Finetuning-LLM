with store_stats as (
    select store_id, max(sale_date) as latest_sale_date
    from store_sales
    where product_id in (1, 2, 4, 5)
    group by store_id
)
select s.id, s.sqft, ss.latest_sale_date
from stores s
left join store_stats ss on s.id = ss.store_id;
