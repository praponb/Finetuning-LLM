select
    products.id,
    products.price,
    products.cost,
    products.brand
from products
where products.supplier_id in (7, 23, 33, 40) and products.supplier_id in (select id from suppliers where rating > 2.3)
