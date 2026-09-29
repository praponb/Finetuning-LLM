select id, category, explode(tags) as tag from products where supplier_id is not null;
