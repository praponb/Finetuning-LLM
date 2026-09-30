select p.brand, p.supplier_id, round(avg(oi.unit_price), 2) as avg_unit_price
from order_items oi
left join products p on oi.product_id = p.id
group by p.brand, p.supplier_id
