select p.id, p.brand, t.inventory_count from products p join (select product_id, count(*) inventory_count from inventory group by product_id) t on p.id = t.product_id;
