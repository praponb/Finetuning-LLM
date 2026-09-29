select
    p.id,
    p.price,
    t.min_revenue,
    t.units_count
from products p
left join (select product_id, min(revenue) as min_revenue, count(units) as units_count
    from store_sales
    group by product_id) t on p.id = t.product_id
where t.min_revenue > 3000
order by min_revenue;
