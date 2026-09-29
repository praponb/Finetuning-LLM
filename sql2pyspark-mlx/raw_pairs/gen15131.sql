select
    i.id,
    i.quantity,
    p.price,
    s.city,
    s.country,
    ceil(i.quantity) as quantity_whole
from inventory i
left join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where s.city = 'New York'
order by city
