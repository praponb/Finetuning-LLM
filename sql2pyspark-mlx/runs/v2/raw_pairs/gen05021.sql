select
    i.id,
    i.last_restock,
    i.store_id,
    p.supplier_id,
    p.stock_qty,
    s.country,
    s.city,
    round(s.rating * 0.85, 2) as adjusted_rating
from inventory i
inner join products p on i.product_id = p.id
left join suppliers s on p.supplier_id = s.id
order by i.last_restock desc, id
limit 5;
