select oi.unit_price, p.category, p.brand
from order_items oi
left join products p on oi.product_id = p.id
where p.supplier_id is not null or p.price > 420
order by p.category;
