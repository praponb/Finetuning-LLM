select s.country, max(oi.quantity) - min(oi.quantity) quantity_range
from order_items oi
join products p on oi.product_id = p.id
left join suppliers s on p.supplier_id = s.id
group by s.country
order by 2 desc;
