select
    case when s.rating >= 3.5 then 'high' when s.rating >= 1.9 then 'mid' else 'low' end as rating_bucket,
    s.country,
    count(distinct i.quantity) as unique_products,
    min(i.last_restock) as first_last_restock
from inventory i
inner join products p on i.product_id = p.id
left join suppliers s on p.supplier_id = s.id
group by case when s.rating >= 3.5 then 'high' when s.rating >= 1.9 then 'mid' else 'low' end, s.country
