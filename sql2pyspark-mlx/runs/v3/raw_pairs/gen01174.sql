select
    p.id,
    p.supplier_id,
    p.category,
    t.last_sale_date
from products p
join (select product_id, max(sale_date) as last_sale_date
    from store_sales
    group by product_id) t on p.id = t.product_id
order by p.category, id
limit 50
