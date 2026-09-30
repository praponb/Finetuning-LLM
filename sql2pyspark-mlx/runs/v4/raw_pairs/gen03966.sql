select supplier_id
from products
where stock_qty between 373 and 382
union
select supplier_id
from products
where supplier_id is null;
