select quantity, product_id, round(quantity, 1) as quantity_rounded
from inventory
where product_id in (1, 3, 7, 10)
order by product_id desc, quantity;
