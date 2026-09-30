select id, stock_qty, category
from products
where array_contains(tags, 'premium')
order by id desc, stock_qty
