select i.store_id, p.price, p.stock_qty
from inventory i
inner join products p on i.product_id = p.id;
