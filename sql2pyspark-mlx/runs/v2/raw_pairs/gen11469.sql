select products.id, products.category from products where products.supplier_id in (select id from suppliers where city = 'Berlin') order by products.id desc;
