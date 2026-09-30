select
    s.store_name,
    s.city,
    t.max_revenue,
    t.last_sale_date
from stores s
left outer join (select store_id, max(revenue) as max_revenue, max(sale_date) as last_sale_date
    from store_sales
    where store_id in (1, 2, 4, 6)
    group by store_id) t on s.id = t.store_id
order by s.city asc, store_name asc;
