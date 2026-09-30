select id, supplier_id, stock_qty
from products
where stock_qty != 178
except
select id, supplier_id, stock_qty
from products
where supplier_id in (3, 6, 12);
