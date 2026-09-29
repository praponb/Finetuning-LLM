select
    i.quantity,
    i.last_restock,
    i.product_id,
    p.stock_qty,
    p.category
from inventory i
join products p on p.id = i.product_id
where p.stock_qty < 247 and p.price between 570 and 670
order by category asc
