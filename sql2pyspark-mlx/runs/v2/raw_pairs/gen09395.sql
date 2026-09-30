select p.id, p.product_name, p.category, p.brand from products p where p.cost <= 710 and p.supplier_id in (select id from suppliers where rating between 2.8 and 3.5)
