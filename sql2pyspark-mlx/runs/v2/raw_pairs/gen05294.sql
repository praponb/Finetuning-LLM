select
    p.id,
    p.supplier_id,
    p.cost,
    t.total_count
from products p
left join (select product_id, count(*) as total_count
    from store_sales
    group by product_id) t on p.id = t.product_id
where t.total_count > 4
order by cost;
